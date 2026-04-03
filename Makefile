IMAGE := tables-explorer

search:
	python search_schema.py --interactive --fk --pretty

streamlit:
	streamlit run app.py --server.port 9234

docker-build:
	docker build -t $(IMAGE) .

docker-run:
	docker run --rm -p 9234:9234 $(IMAGE)

docker-run-cli:
	docker run -it --rm $(IMAGE) python search_schema.py --interactive --fk --pretty
