"""

"""

import datalad.distributed.create_sibling_gitlab as mod_gitlab
# provide some symbols from it for the patch below
CapturedException = mod_gitlab.CapturedException
GitLabSite = mod_gitlab.GitLabSite
known_access_labels = mod_gitlab.known_access_labels
lgr = mod_gitlab.lgr

from datalad_next.commands import build_doc

from . import apply_patch

known_layout_labels = ('collection', 'flat')

command_doc = """
    Create dataset sibling at a GitLab site

    An existing GitLab project, or a project created via the GitLab web
    interface can be configured as a sibling with the :command:`siblings`
    command. Alternatively, this command can create a GitLab project at any
    location/path a given user has appropriate permissions for. This is
    particularly helpful for recursive sibling creation for subdatasets. API
    access and authentication are implemented via python-gitlab, and all its
    features are supported. A particular GitLab site must be configured in a
    named section of a python-gitlab.cfg file (see
    https://python-gitlab.readthedocs.io/en/stable/cli.html#configuration for
    details), such as::

      [mygit]
      url = https://git.example.com
      api_version = 4
      private_token = abcdefghijklmnopqrst

    Subsequently, this site is identified by its name ('mygit' in the example
    above).

    (Recursive) sibling creation for all, or a selected subset of subdatasets
    is supported with two different project layouts (see --layout):

    "flat"
      All datasets are placed as GitLab projects in the same group. The project name
      of the top-level dataset follows the configured
      datalad.gitlab-SITENAME-project configuration. The project names of
      contained subdatasets extend the configured name with the subdatasets'
      s relative path within the root dataset, with all path separator
      characters replaced by '-'. This path separator is configurable
      (see Configuration).
    "collection"
      A new group is created for the dataset hierarchy, following the
      datalad.gitlab-SITENAME-project configuration. The root dataset is placed
      in a "project" project inside this group, and all nested subdatasets are
      represented inside the group using a "flat" layout. The root datasets
      project name is configurable (see Configuration).
      This command cannot create root-level groups! To use this layout for
      a collection located in the root of an account, create the target
      group via the GitLab web UI first.

    GitLab cannot host dataset content. However, in combination with
    other data sources (and siblings), publishing a dataset to GitLab can
    facilitate distribution and exchange, while still allowing any dataset
    consumer to obtain actual data content from alternative sources.

    *Configuration*

    Many configuration switches and options for GitLab sibling creation can
    be provided arguments to the command. However, it is also possible to
    specify a particular setup in a dataset's configuration. This is
    particularly important when managing large collections of datasets.
    Configuration options are:

    "datalad.gitlab-default-site"
        Name of the default GitLab site (see --site)
    "datalad.gitlab-SITENAME-siblingname"
        Name of the sibling configured for the local dataset that points
        to the GitLab instance SITENAME (see --name)
    "datalad.gitlab-SITENAME-layout"
        Project layout used at the GitLab instance SITENAME (see --layout)
    "datalad.gitlab-SITENAME-access"
        Access method used for the GitLab instance SITENAME (see --access)
    "datalad.gitlab-SITENAME-project"
        Project "location/path" used for a datasets at GitLab instance
        SITENAME (see --project). Configuring this is useful for deriving
        project paths for subdatasets, relative to superdataset.
        The root-level group ("location") needs to be created beforehand via
        GitLab's web interface.
    "datalad.gitlab-default-projectname"
        The collection layout publishes (sub)datasets as projects
        with a custom name. The default name "project" can be overridden with
        this configuration.
    "datalad.gitlab-default-pathseparator"
        The flat and collection layout represent subdatasets with project names
        that correspond to the path, with the regular path separator replaced
        with a "-": superdataset-subdataset. This configuration can override
        this default separator.

    This command can be configured with
    "datalad.create-sibling-ghlike.extra-remote-settings.NETLOC.KEY=VALUE" in
    order to add any local KEY = VALUE configuration to the created sibling in
    the local `.git/config` file. NETLOC is the domain of the Gitlab instance to
    apply the configuration for.
    This leads to a behavior that is equivalent to calling datalad's
    ``siblings('configure', ...)``||``siblings configure`` command with the
    respective KEY-VALUE pair after creating the sibling.
    The configuration, like any other, could be set at user- or system level, so
    users do not need to add this configuration to every sibling created with
    the service at NETLOC themselves.

    """


#
# This replacement function is taken from
# https://github.com/datalad/datalad/pull/7410
# @7c83f4ac282dc3b48be8439dbbbe0f0c2c57d467
# The actual change over the patch target is only four lines, but the spaghetti
# nature of the function does not allow for a lean patch.
def _proc_dataset(refds, ds, site, project, remotename, layout, existing,
                  access, dry_run, siteobjs, depends, description):
    # basic result setup
    res_kwargs = dict(
        action='create_sibling_gitlab',
        refds=refds.path,
        path=ds.path,
        type='dataset',
        logger=lgr,
    )
    if description:
        res_kwargs['description'] = description

    if site is None:
        # always try pulling the base config from a parent dataset
        # even if paths were given (may be overwritten later)
        basecfgsite = ds.config.get('datalad.gitlab-default-site', None)

    # let the dataset config overwrite the target site, if none
    # was given
    site = refds.config.get(
        'datalad.gitlab-default-site', basecfgsite) \
        if site is None else site
    if site is None:
        # this means the most top-level dataset has no idea about
        # gitlab, and no site was specified as an argument
        # fail rather then give an error result, as this is very
        # unlikely to be intentional
        raise ValueError(
            'No GitLab site was specified (--site) or configured '
            'in {} (datalad.gitlab.default-site)'.format(ds))
    res_kwargs['site'] = site

    # determine target remote name, unless given
    if remotename is None:
        remotename_var = 'datalad.gitlab-{}-siblingname'.format(site)
        remotename = ds.config.get(
            remotename_var,
            # use config from parent, if needed
            refds.config.get(
                remotename_var,
                # fall back on site name, if nothing else can be used
                site))
    res_kwargs['sibling'] = remotename
    # check against existing remotes
    dremotes = {
        r['name']: r
        for r in ds.siblings(
            action='query',
            # fastest possible
            get_annex_info=False,
            recursive=False,
            return_type='generator',
            result_renderer='disabled')
    }
    if remotename in dremotes and existing not in ['replace', 'reconfigure']:
        # we already know a sibling with this name
        yield dict(
            res_kwargs,
            status='error' if existing == 'error' else 'notneeded',
            message=('already has a configured sibling "%s"', remotename),
        )
        return

    if layout is None:
        # figure out the layout of projects on the site
        # use the reference dataset as default, and fall back
        # on 'collection' as the most generic method of representing
        # the filesystem in a group/subproject structure
        layout_var = 'datalad.gitlab-{}-layout'.format(site)
        layout = ds.config.get(
            layout_var, refds.config.get(
                layout_var, 'collection'))
    if layout not in known_layout_labels:
        raise ValueError(
            "Unknown site layout '{}' given or configured, "
            "known ones are: {}".format(layout, known_layout_labels))

    if access is None:
        access_var = 'datalad.gitlab-{}-access'.format(site)
        access = ds.config.get(
            access_var, refds.config.get(
                access_var, 'http'))
    if access not in known_access_labels:
        raise ValueError(
            "Unknown site access '{}' given or configured, "
            "known ones are: {}".format(access, known_access_labels))

    pathsep = ds.config.get("datalad.gitlab-default-pathseparator", None) or "-"
    project_stub = \
        ds.config.get("datalad.gitlab-default-projectname", None) or "project"
    project_var = 'datalad.gitlab-{}-project'.format(site)
    process_root = refds == ds
    if project is None:
        # look for a specific config in the dataset
        project = ds.config.get(project_var, None)

    if project and process_root and layout != 'flat':
        # the root of a collection
        project = f'{project}/{project_stub}'
    elif project is None and not process_root:
        # check if we can build one from the refds config
        ref_project = refds.config.get(project_var, None)
        if ref_project:
            # layout-specific derivation of a path from
            # the reference dataset configuration
            rproject = ds.pathobj.relative_to(refds.pathobj).as_posix()
            if layout == 'collection':
                project = '{}/{}'.format(
                    ref_project,
                    rproject.replace('/', pathsep))
            else:
                project = '{}-{}'.format(
                    ref_project,
                    rproject.replace('/', pathsep))

    if project is None:
        yield dict(
            res_kwargs,
            status='error',
            message='No project name/location specified, and no configuration '
                    'to derive one',
        )
        return

    res_kwargs['project'] = project

    if dry_run:
        # this is as far as we can get without talking to GitLab
        yield dict(
            res_kwargs,
            status='ok',
            dryrun=True,
        )
        return

    # and now talk to GitLab for real
    site_api = siteobjs[site] if site in siteobjs else GitLabSite(site)

    site_project = site_api.get_project(project)
    if site_project is None:
        try:
            site_project = site_api.create_project(project, description)
            # report success
            message = "sibling repository '%s' created at %s",\
                      remotename, site_project.get('web_url', None)
            yield dict(
                res_kwargs,
                # relay all attributes
                project_attributes=site_project,
                message=message,
                status='ok',
            )
        except Exception as e:
            ce = CapturedException(e)
            yield dict(
                res_kwargs,
                # relay all attributes
                status='error',
                message=('Failed to create GitLab project: %s', ce),
                exception=ce
            )
            return
    else:
        # there already is a project
        if existing == 'error':
            # be nice and only actually error if there is a real mismatch
            if remotename not in dremotes:
                yield dict(
                    res_kwargs,
                    project_attributes=site_project,
                    status='error',
                    message=(
                        "There is already a project at '%s' on site '%s', "
                        "but no sibling with name '%s' is configured, "
                        "maybe use --existing=reconfigure",
                        project, site, remotename,
                    )
                )
                return
            elif access in ('ssh', 'ssh+http') \
                    and dremotes[remotename].get(
                        'url', None) != site_project.get(
                            # use False as a default so that there is a
                            # mismatch, complain if both are missing
                            'ssh_url_to_repo', False):
                yield dict(
                    res_kwargs,
                    project_attributes=site_project,
                    status='error',
                    message=(
                        "There is already a project at '%s' on site '%s', "
                        "but SSH access URL '%s' does not match '%s', "
                        "maybe use --existing=reconfigure",
                        project, site,
                        dremotes[remotename].get('url', None),
                        site_project.get('ssh_url_to_repo', None)
                    )
                )
                return
            elif access == 'http' \
                    and dremotes[remotename].get(
                        'url', None) != site_project.get(
                            # use False as a default so that there is a
                            # mismatch, veen if both are missing
                            'http_url_to_repo', False):
                yield dict(
                    res_kwargs,
                    project_attributes=site_project,
                    status='error',
                    message=(
                        "There is already a project at '%s' on site '%s', "
                        "but HTTP access URL '%s' does not match '%s', "
                        "maybe use --existing=reconfigure",
                        project, site,
                        dremotes[remotename].get('url', None),
                        site_project.get('http_url_to_repo', None)
                    )
                )
                return
        yield dict(
            res_kwargs,
            project_attributes=site_project,
            status='notneeded',
            message=(
                "There is already a project at '%s' on site '%s'",
                project, site,
            )
        )

    # first make sure that annex doesn't touch this one
    # but respect any existing config
    ignore_var = 'remote.{}.annex-ignore'.format(remotename)
    if ignore_var not in ds.config:
        ds.config.add(ignore_var, 'true', scope='local')

    for res in ds.siblings(
            'configure',
            name=remotename,
            url=site_project['http_url_to_repo']
            if access in ('http', 'ssh+http')
            else site_project['ssh_url_to_repo'],
            pushurl=site_project['ssh_url_to_repo']
            if access in ('ssh', 'ssh+http')
            else None,
            recursive=False,
            publish_depends=depends,
            result_renderer='disabled',
            return_type='generator'):
        yield res


apply_patch(
    'datalad.distributed.create_sibling_gitlab',
    None,
    '_proc_dataset',
    _proc_dataset)
apply_patch(
    'datalad.distributed.create_sibling_gitlab', None, 'known_layout_labels',
    known_layout_labels,
    msg='Stop advertising discontinued "hierarchy" layout for '
    '`create_siblign_gitlab()`')
# also put in effect for the constraint, add None to address limitation that
# the default also needs to be covered for datalad-core
mod_gitlab.CreateSiblingGitlab._params_['layout'].constraints._allowed = \
    (None,) + known_layout_labels

# rebuild command docs
mod_gitlab.CreateSiblingGitlab.__call__.__doc__ = None
mod_gitlab.CreateSiblingGitlab.__doc__ = command_doc
mod_gitlab.CreateSiblingGitlab = build_doc(mod_gitlab.CreateSiblingGitlab)
