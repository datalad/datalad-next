from typing import (
    Tuple,
)

from urllib.parse import urlparse

from fsspec.core import url_to_fs

from botocore.exceptions import NoCredentialsError

from datalad_next.utils.credman import CredentialManager

from . import (
    UrlOperationsAuthorizationError,
    UrlOperationsRemoteError,
)


def get_fs(url, target_url, *, cfg, credential, **kwargs) -> Tuple:
    """Get filesystem from s3:// URL

    If no ``credential`` identifier is provided, a first access attempt is made
    without provisioning any DataLad-based credentials. This enables the
    underlying botocore package to look for credentials on its own (which, for
    example, enables "just-works" when running on AWS EC2 instances), unless
    anonymous access was requested explicitly. Any ``**kwargs`` are passed
    to ``fsspec.core.url_to_fs()``.

    If access without explicitly provisioned credentials fails with a
    permission error, a credential is looked up or prompted for, and a second
    attempt with non-anonymous access and the queried credential is made.
    """
    if url == target_url:
        # no chain, pull any 's3' specific arguments to the top-level
        kwargs = dict(
            kwargs.get('s3', {}),
            **{k: v for k, v in kwargs.items() if k != 's3'}
        )
    object_url = target_url
    s3bucket_name = urlparse(object_url).netloc

    # if there are instructions to use an explicit datalad-based credential,
    # there is no point in trying anon-access or any of boto's internal
    # mechanism
    if not credential:
        try:
            # we start with no explicit credentials. This will cause boto to
            # use anonymous or some credentials stored in the config files it
            # recognizes, or use the respective env vars (incl session tokens).
            # This approach will yield the highest efficiency across a diverse
            # set of use cases.
            # note that if anonymous access is desired (on this first attempt),
            # it must be enabled by adding `anon=True` to the `kwargs`.
            return _get_fs(url, **kwargs)
        except PermissionError as e:
            # TODO log this error
            # access without credential provisioning failed. this could mean
            # different things:
            # - credentials are deployed externally via env vars, but anonymous
            #   access (anon=True) would be needed, because the credentials do
            #   not match
            # - credentionals are needed and provisioned, but are wrong
            pass
        except NoCredentialsError as e:
            # TODO log this error
            # credentials are known to be required needed by not available
            pass
        except Exception as e:
            # something unexpected, reraise
            raise UrlOperationsRemoteError(object_url) from e

    # if we get here, access failed in a credential-related fashion.
    # try to determine credentials for this target bucket.

    # compose a standard realm identifer
    # TODO recognize alternative endpoints here
    host = 's3.amazonaws.com'
    # this is the way AWS exposes bucket content via https in the "virtualhost"
    # fashion, but we stick to s3:// to avoid confusion. Therefore we are also
    # not adding the bucketname as the first component of the URL path (where
    # it would be in real s3:// URLs, instead of the host).  Taken together we
    # get a specialization of an S3 realm that is endpoint/service specific
    # (hence we do not confuse AWS credentials with those of a private MinIO
    # instance).
    # TODO it is not 100% clear to mih whether a credential would
    # always tend to be for a bucket-wide scope, or whether per-object
    # credentials are a thing
    realm = f's3://{s3bucket_name}.{host}'

    credman = CredentialManager(cfg)
    credname, cred = credman.obtain(
        credential,
        prompt=f'Credential required to access {object_url}',
        query_props=dict(realm=realm),
        type_hint='s3',
        expected_props=['key', 'secret'],
    )

    credprops = dict(key=cred['key'], secret=cred['secret'])
    if object_url == url:
        # s3 is the main target, all args are at the top-level
        kwargs.update(anon=False, **credprops)
    else:
        # s3 is just one of the filesystems needed in a chain
        # look for any provided s3 config and update for explicit
        # credential provision
        kwargs.update(s3=dict(kwargs.get('s3', {}), anon=False, **credprops))

    # now try again, this time with a credential
    try:
        fs_url_stat = _get_fs(url, **kwargs)
    except PermissionError as e:
        raise UrlOperationsAuthorizationError(object_url) from e
    except Exception as e:
        raise UrlOperationsRemoteError(object_url) from e
    # if we get here, we have a working credential, store it
    # (will be skipped without a given name after possibly
    # prompting for one)
    credman.set(
        # use given name, will prompt if none
        credname,
        # make lookup of most recently used credential for the realm
        # (the bucket) possible
        _last_used=True,
        _context=f'for accessing {realm}',
        **cred
    )
    return fs_url_stat


def _get_fs(url, **kwargs):
    fs, urlpath = url_to_fs(url, **kwargs)
    # check proper functioning
    stat = fs.stat(urlpath)
    return fs, urlpath, stat
