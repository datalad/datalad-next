import requests

from ..requests_auth import DataladAuth


def test_token_credential_uses_bearer_with_basic_only_challenge():
    response = requests.Response()
    response.status_code = 401
    response.url = 'https://example.com/file'
    response.headers['www-authenticate'] = 'Basic realm=""'

    auth = DataladAuth.__new__(DataladAuth)

    def get_credential(url, auth_schemes):
        assert url == response.url
        assert auth_schemes == {'basic': {'realm': ''}}
        return None, 'mytoken', {
            'type': 'token',
            'secret': 'sekrit',
        }

    rerequest = {}

    def authenticated_rerequest(response_, request_auth, **kwargs):
        rerequest['response'] = response_
        prep = requests.Request('GET', response_.url).prepare()
        request_auth(prep)
        return prep

    auth._get_credential = get_credential
    auth._authenticated_rerequest = authenticated_rerequest

    renewed_request = auth.handle_401(response)

    assert rerequest['response'] is response
    assert renewed_request.headers['Authorization'] == 'Bearer sekrit'
