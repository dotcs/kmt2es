# Komoot-to-ElasticSearch Tour Importer

Tool that helps to download tour data from Komoot and import it into an
ElasticSearch database.

## Getting started

This tool works with the Komoot API and needs to authenticate against the
API. You need to log into the [web UI](https://komoot.de) and obtain your
user id by clicking on your profile and extract the ID from the URL, e.g.
given this URL `https://www.komoot.de/user/1234567890` your user id would be
`1234567890`.

To call the API as this user you need to obtain your session information by
having a look at a request, e.g. in [Firefox Storage
Inspector](https://developer.mozilla.org/de/docs/Tools/Storage_Inspector#Cookies).
This should give you three cookies: `kmt_auth`, `kmt_session` and
`kmt_session.sig`.

Then store those values as bash variables:

```bash
# Enter your values here
KOMOOT_USER_ID="<...>"
KOMOOT_COOKIE="kmt_auth=<...>; kmt_session=<...>; kmt_session.sig=<...>"
```

### Create and run docker container

This repository contains a Dockerfile that allows to build a docker container
that comes with all necessary tools pre-installed:

```bash
docker build -t komoot-importer:latest .
docker run --rm -it komoot-importer \
    --user-id ${KOMOOT_USER_ID} \
    --cookie "${KOMOOT_COOKIE}" \
    --elasticsearch-host http://localhost:9200
```