build-s3DropBucketDependencyLayer:
	rm -rf .aws-sam
	mkdir nodejs
	cp layers/node_modules nodejs/
	zip -r nodejs/ nodejs.zip
	mkdir -p "$(ARTIFACTS_DIR)/nodejs/"
	cp nodejs.zip "$(ARTIFACTS_DIR)/nodejs"
