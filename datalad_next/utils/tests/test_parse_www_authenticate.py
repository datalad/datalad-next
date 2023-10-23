
from ..requests_auth import parse_www_authenticate


challenges = (
    # just challenge type
    ('Negotiate',
     [('negotiate', None)]),
    # challenge and just a token, tolerate any base64 padding
    ('Negotiate abcdef',
     [('negotiate', 'abcdef')]),
    ('Negotiate abcdef=',
     [('negotiate', 'abcdef=')]),
    ('Negotiate abcdef==',
     [('negotiate', 'abcdef==')]),
    # standard bearer
    ('Bearer realm=example.com',
     [('bearer', {'realm': 'example.com'})]),
    # standard digest
    ('Digest realm="example.com", qop="auth,auth-int", nonce="abcdef", '
     'opaque="ghijkl"',
     [('digest', {'realm': 'example.com', 'qop': 'auth,auth-int',
                  'nonce': 'abcdef', 'opaque': 'ghijkl'})]),
    # multi challenge
    ('Basic speCial="paf ram", realm="basIC", '
     'Bearer, '
     'Digest realm="http-auth@example.org", qop="auth, auth-int", '
     'algorithm=MD5',
     [('basic', {'special': 'paf ram', 'realm': 'basIC'}),
      ('bearer', None),
      ('digest', {'realm': "http-auth@example.org", 'qop': "auth, auth-int",
                  'algorithm': 'MD5'})]),
    # same challenge, multiple times, last one wins
    ('Basic realm="basIC", '
     'Basic realm="complex"',
     [('basic', {'realm': 'complex'})]),
)


def test_parse_www_authenticate():
    for hdr, targets in challenges:
        res = parse_www_authenticate(hdr)
        for ctype, props in targets:
            assert ctype in res
            assert res[ctype] == props
