from urllib.parse import urlparse, parse_qs
__author__ = 'vvf'

def get_params_from_url(url):
    url_string = url
    url = urlparse(url)
    qs = ''

    # in python2.6, custom URL schemes don't recognize querystring values
    # they're left as part of the url.path.
    if '?' in url.path and not url.query:
        # chop the querystring including the ? off the end of the url
        # and reparse it.
        qs = url.path.split('?', 1)[1]
        url = urlparse(url_string[:-(len(qs) + 1)])
    else:
        qs = url.query

    url_options = {}

    for name, value in parse_qs(qs):
        if value and len(value) > 0:
            url_options[name] = value[0]

    # We only support redis:// and unix:// schemes.
    if url.scheme == 'unix':
        url_options.update({
            'password': url.password,
            'path': url.path,
        })

    else:
        url_options.update({
            'host': url.hostname,
            'port': int(url.port or 6379),
            'password': url.password,
        })

        # If there's a path argument, use it as the db argument if a
        # querystring value wasn't specified
        if 'db' not in url_options and url.path:
            try:
                url_options['db'] = int(url.path.replace('/', ''))
            except (AttributeError, ValueError):
                pass

    return url_options
