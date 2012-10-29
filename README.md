django-elliptics
================

This module provides scalable file storage backend for Django. It leverages [Elliptics](/reverbrain/elliptics) and [elliptics-fastcgi](/lmovsesjan/elliptics-fastcgi).

Configuration
-------------
The storage backend is called `django_elliptics.storage.EllipticsStorage`.

django-elliptics uses these configuration settings:

 * `ELLIPTICS_PUBLIC_URL` - base URL for the public elliptics-fastcgi node. E.g. _http://uploads.myproject.com_. It should be accessible to the end users.
 * `ELLIPTICS_PRIVATE_URL` - base URL for the modification requests. E.g. _http://localhost:9000_.
 * `ELLIPTICS_PREFIX` - a prefix to add to all names before storing files. Allows to avoid conflicts when sharing storage between applications.

You can also set these using `public_url` and `private_url` arguments to the EllipticsStorage constructor.
