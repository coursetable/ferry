# Workflow

We want the crawler to be reproducible and reliable. As such, we designed the crawling pipeline as a number of stages able to be independently inspected and rerun.

1. **Retrieval**: We pull and preprocess raw data from Yale's websites to fetch the following:

   - Course listings
   - Course demand statistics
   - Course evaluations

2. **Preprocessing**: We preprocess course listing files and evaluations to make them easier to import.

3. **Importation**: We import the preprocessed data files into our Postgres database.

Retrieval is documented in the [retrieval docs](docs/1_retrieval.md) and implemented in the `/ferry/crawler` directory along with preprocessing. We also needed to migrate data from the previous CourseTable databases in a similar fashion. This process is documented in the [migration docs](docs/0_migration.md) and implemented in the `/ferry/migration` directory.

Importation and post-processing make use of the database, which is documented in [parsing docs](docs/2_parsing.md). Moreover, the database schema is defined with SQLAlchemy in `/ferry/database/models.py`. A key part of importation is the identification of past offerings of a class, which is detailed [here](docs/3_same_classes.md).

In the middle of importing our tables, we also generate FastText and TF-IDF embeddings of courses based on the text contained in titles and descriptions, which helps us recommend similar courses. This embedding workflow is described [here](docs/4_embedding.md).

In general, importation is a three-step process:

1. **Transforming:** We first pull everything together in `/ferry/transform.py`, where we use Pandas to construct tables from various preprocessed files. These are then saved to CSVs per table in `/data/importer_dumps/`. These tables are intended to mirror the SQLAlchemy schema.
2. **Staging**: In `/ferry/stage.py`, we read the previously-generated CSVs and upload them to staging tables (identified with a `_staged` suffix) in the database. Note that the schema itself describes the tables with `_staged` prefixes that are removed after deployment.
3. **Deploying**: staged tables are then validated in `/ferry/deploy.py`, after which they are pushed to the main tables if all checks are successful. Both importation and post-processing are fully idempotent. Note that `deploy.py` **must** be run after `stage.py` each time, as it upgrades the staging tables by renaming them to main ones. Because the staging tables completely replace the main ones in each deployment, schema updates are relatively easy – they only have to be defined once in `/ferry/database/models.py`, after which running the import pipeline will have them take effect.
