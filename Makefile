.PHONY: up down down-with-volumes

.env: .env.template
	cp $< $@

up: .env
	docker compose up --build -d

down:
	docker compose down

down-with-volumes:
	docker compose down --volumes
