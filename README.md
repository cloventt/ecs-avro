# Elastic Common Avro Schema

This repo contains an [Apache Avro](https://avro.apache.org) schema derived from the "flat" 
version of the [Elastic Common Schema](https://www.elastic.co/guide/en/ecs/current/index.html).
It also includes a Python tool for converting the Elastic-provided YAML format into the .avsc 
JSON format.

The schema is in the file [elastic-common-schema.avsc](./elastic-common-schema.avsc). You can use
Git tags to select which version of the ECS you would like to use.

## Script Usage
When checking out this repo, ensure that the https://github.com/elastic/ecs/ repo is checked out under
a sub-folder called `ecs`. This might happen automatically, but if not run `git submodule update` to pull 
it down.

To use the script, you will need Python 3.9 or later. Set up a virtualenv, install the dependencies
(`pip install -r requirements.txt`) and then run the script (`python main.py`).