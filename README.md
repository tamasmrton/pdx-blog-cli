# pdx-blog scraper

## This is a python project that scrapes developer diaries of Paradox games.

### Description

I am a big fan of Paradox and the way they communicate with the community. This mini-project offers a way to fetch developer diaries of any Paradox game and store it `locally` or on an ` S3 bucket`.

To decide which game's diaries you want, you can either specify directly using the `--game` option in the cli OR use the command prompt and select the desired game from a list.

### Instructions

#### 1. How to set up the project locally

- clone this repository
- create a venv and run `pip install -r requirements.txt`
- for storing developer diaries in an S3 bucket, define the following environment variables:

  | Variable name  |                 Description |
  | :------------- | --------------------------: |
  | AWS_ACCESS_KEY |        Access key to bucket |
  | AWS_SECRET_KEY | Secret access key to bucket |
  | AWS_BUCKET     |          Name of the bucket |

- run the script with cli: `python app.py --game <GAME_NAME> --destination <local/s3>`
- run the script with command prompt: `python app.py --destination <local/s3>`

#### 2. How to set up the project with docker

- make sure you have docker installed
- create a `credentials.env` file containing the S3 bucket secrets
- build the docker image: `docker build . --tag pdx-blog`
- run container: `docker run --env-file ./credentials.env pdx-blog --game "<GAME_NAME>" --destination <local/s3>`
