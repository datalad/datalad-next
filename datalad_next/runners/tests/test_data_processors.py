from __future__ import annotations

import json
from itertools import chain

from ..data_processor_pipeline import (
    DataProcessorPipeline,
    process_from,
)
from ..data_processors.decode import decode_processor
from ..data_processors.jsonline import jsonline_processor
from ..data_processors.pattern import pattern_processor
from ..data_processors.splitlines import splitlines_processor


decode_utf8_processor = decode_processor()

text = '''This is the first line of text
the second line of text, followed by an empty line

4th line of text with some non-ASCII characters: äöß


{"key0": "some text \\u1822"}

7th line with interesting characters: € 😃👽
an a non-terminated line'''

text_lines = text.splitlines(keepends=True)
text_data_chunks = [
    text.encode()[i:i+100]
    for i in range(0, len(text.encode()) + 100, 100)
]


json_result = [
    (True, {'key1': 'simple'}),
    (True, {'key2': 'abc\naabböäß'}),
    (True, {'key3': {'key3.1': 1.2}}),
]

json_text = '\n'.join([json.dumps(o[1]) for o in json_result])
json_data_chunks = [
    json_text.encode()[i:i+100]
    for i in range(0, len(json_text.encode()) + 100, 100)
]


def test_decoding_splitting():
    result = [
        line
        for line in process_from(
            data_source=text_data_chunks,
            processors=[
                decode_utf8_processor,
                splitlines_processor()
            ]
        )
    ]
    assert result == text_lines


def test_json_lines():
    result = [
        json_info
        for json_info in process_from(
            data_source=json_data_chunks,
            processors=[
                decode_utf8_processor,
                splitlines_processor(),
                jsonline_processor
            ]
        )
    ]
    assert result == json_result


def test_faulty_json_lines():
    result = [
        json_info[1]
        for json_info in process_from(
            data_source=text_data_chunks,
            processors=[
                decode_utf8_processor,
                splitlines_processor(),
                jsonline_processor
            ]
        )
        if json_info[0] is True
    ]
    assert len(result) == 1
    assert result[0] == {'key0': 'some text \u1822'}


def test_pattern_border_processor():
    from ..data_processors import pattern_processor

    def perform_test(data_chunks: list[str | bytes],
                     pattern: str | bytes,
                     expected_non_final: tuple[list[str | bytes], list[str | bytes]],
                     expected_final: tuple[list[str | bytes], list[str | bytes]]):

        copied_data_chunks = data_chunks[:]
        for final, result in ((True, expected_final), (False, expected_non_final)):
            r = pattern_processor(pattern)(data_chunks, final=final)
            assert tuple(r) == result, f'failed with final {final}'
            # Check that the original list was not modified
            assert copied_data_chunks == data_chunks

    perform_test(
        data_chunks=['a', 'b', 'c', 'd', 'e'],
        pattern='abc',
        expected_non_final=(['abc', 'de'], []),
        expected_final=(['abc', 'de'], []),
    )

    perform_test(
        data_chunks=['a', 'b', 'c', 'a', 'b', 'c'],
        pattern='abc',
        expected_non_final=(['abc', 'abc'], []),
        expected_final=(['abc', 'abc'], []),
    )

    # Ensure that unaligned pattern prefixes are not keeping data chunks short
    perform_test(
        data_chunks=['a', 'b', 'c', 'dddbbb', 'a', 'b', 'x'],
        pattern='abc',
        expected_non_final=(['abc', 'dddbbb', 'abx'], []),
        expected_final=(['abc', 'dddbbb', 'abx'], []),
    )

    # Expect that a trailing minimum length-chunk that ends with a pattern
    # prefix is not returned as data, but as remainder, if it is not the final
    # chunk
    perform_test(
        data_chunks=['a', 'b', 'c', 'd', 'a'],
        pattern='abc',
        expected_non_final=(['abc'], ['da']),
        expected_final=(['abc', 'da'], []),
    )

    # Expect the last chunk to be returned as data, if final is True, although
    # it ends with a pattern prefix. If final is false, the last chunk will be
    # returned as a remainder, because it ends with a pattern prefix.
    perform_test(
        data_chunks=['a', 'b', 'c', 'dddbbb', 'a'],
        pattern='abc',
        expected_non_final=(['abc', 'dddbbb'], ['a']),
        expected_final=(['abc', 'dddbbb', 'a'], [])
    )


    perform_test(
        data_chunks=['a', 'b', 'c', '9', 'a'],
        pattern='abc',
        expected_non_final=(['abc'], ['9a']),
        expected_final=(['abc', '9a'], [])
    )


def test_processor_removal():

    stream = iter([b'\1', b'\2', b'\3', b'9\1', b'content'])

    pattern = b'\1\2\3'
    pipeline = DataProcessorPipeline([pattern_processor(pattern)])
    filtered_stream = pipeline.process_from(stream)

    # The first chunk should start with the pattern, i.e. b'\1\2\3'
    chunk = next(filtered_stream)
    assert chunk[:len(pattern)] == pattern

    # Remove the filter again. The chunk is extended to contain all
    # data that was buffered in the pipeline.
    buffered_chunks = pipeline.finalize()
    chunk = b''.join([chunk[len(pattern):]] + buffered_chunks)

    # The length is transferred now and terminated by b'\x01'.
    while b'\x01' not in chunk:
        chunk += next(stream)

    marker_index = chunk.index(b'\x01')
    expected_size = int(chunk[:marker_index])
    assert expected_size == 9
    chunk = chunk[marker_index + 1:]

    source = chain([chunk], stream) if chunk else stream
    assert b''.join(source) == b'content'


def test_split_decoding():
    encoded = 'ö'.encode('utf-8')
    part_1, part_2 = encoded[:1], encoded[1:]

    # check that incomplete encodings are caught
    decoded, remaining = decode_utf8_processor([part_1])
    assert decoded == []
    assert remaining == [part_1]

    # vreify that the omplete encoding decodes correctly
    decoded, remaining = decode_utf8_processor([part_1, part_2])
    assert decoded == ['ö']
    assert remaining == []


def test_pipeline_finishing():
    encoded = 'ö'.encode('utf-8')
    part_1, part_2 = encoded[:1], encoded[1:]

    pipeline = DataProcessorPipeline([decode_utf8_processor])
    res = pipeline.process(part_1)
    assert res == []
    res = pipeline.finalize()
    assert res == ['\\xc3']
