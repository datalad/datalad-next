import requests
import www_authenticate
from datalad.downloaders.http import DEFAULT_USER_AGENT 


def probe_url(url, timeout=10.0, headers=None):
    """Probe a HTTP(S) URL for redirects and authentication needs

    This functions performs a HEAD request against the given URL,
    while waiting at most for the given timeout duration for
    a server response.

    Parameters
    ----------
    url: str
      URL to probe
    timeout: float, optional
      Maximum time to wait for a server response to the probe
    headers: dict, optional
      Any custom headers to use for the probe request. If none are
      provided, or the provided headers contain no 'user-agent'
      field, the default DataLad user agent is added automatically.

    Returns
    -------
    str or None, dict
      The first value is the URL against the final request was
      performed, after following any redirects and applying
      normalizations.

      The second value is a mapping with a particular set of
      properties inferred from probing the webserver. The following
      key-value pairs are supported:

      - 'is_redirect' (bool), True if any redirection occurred. This
        boolean property is a more accurate test than comparing
        input and output URL
      - 'status_code' (int), HTTP response code (of the final request
        in case of redirection).
      - 'auth' (dict), present if the final server response contained any
        'www-authenticate' headers, typically the case for 401 responses.
        The dict contains a mapping of server-reported authentication
        scheme names (e.g., 'basic', 'bearer') to their respective
        properties (dict). These can be any nature and number, depending
        on the respective authentication scheme. Most notably, they
        may contain a 'realm' property that can be used to determine
        suitable credentials for authentication.

    Raises
    ------
    requests.RequestException
      May raise any exception of the `requests` package, most notably
      `ConnectionError`, `Timeout`, `TooManyRedirects`, etc.
    """
    hdrs = {'user-agent': DEFAULT_USER_AGENT}
    if headers is None:
        headers = hdrs
    elif 'user-agent' not in headers:
        headers.update(hdrs)

    props = {}
    req = requests.head(
        url,
        allow_redirects=True,
        timeout=timeout,
        headers=headers,
    )
    if 'www-authenticate' in req.headers:
        props['auth'] = www_authenticate.parse(
            req.headers['www-authenticate'])
    props['is_redirect'] = True if req.history else False
    props['status_code'] = req.status_code
    return req.url, props
