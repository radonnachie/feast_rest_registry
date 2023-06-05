# REST served FEAST registry

[FEAST](https://github.com/feast-dev/feast) provides a SQL backged registry as its goto production solution.
This registry is further "productionalised" by a REST interface in [this branch](https://github.com/radonnachie/feast/tree/rest_registry), a server for which is presented in this repo.

## Demo

`docker compose build`
`docker compose up`

Then [create](https://docs.feast.dev/getting-started/quickstart#step-2-create-a-feature-repository) the exemplary FEAST project:

`feast init restreg`

Replace the `feature_store.yaml` definition to specify a REST-served registry:

```
registry:
    registry_type: rest
    path: http://localhost:80 # sufficient for the provided docker swarm
```

Run FEAST's exemplary `test_workflow.py`.
