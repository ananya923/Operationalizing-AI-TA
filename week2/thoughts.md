- We probably need a list of requirements
- REDUNDANT: also a clear list of steps on how to access stuff. apparently the data parquet file doesn't get copied fully, and even if it does, it needs to be uploaded to GCP along with the model etc. An LLM can help us figure this out but if students just follow what the llm is telling them without questioning it, they won't understand why things are being done this way.
- LFS pulls don't work, had to download data and modeling file manually from github. probably a requirements / config issue as well, so would be nice to state the requirements clearly for this
- EDITING THE DOCKERFILE PART IS REALLY EFFECTIVE FOR LEARNING
-  got an error while building docker image which might be a requirements issue: "The container is crashing because LightGBM needs a system library called libgomp that isn't included in the slim Python image. We need to add it to the Dockerfile.
Open starter/Dockerfile and add this block in Stage 2 (runtime), right after WORKDIR /app:"
- speaking of docker, WINDOWS USERS FACE A LOT OF ISSUES WITH DOCKER and i do not have windows and don'tknow how to solve those issues.
- got another error for the requests module while building docker image, added that manually to the requirements.txt file
- too many errors with the docker image!!! after one issue gets solved, another one crops up