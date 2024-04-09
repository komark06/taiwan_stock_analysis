.PHONY: up down down-with-volumes

db_assets/security/.complete: db_assets/generate_password.py
	mkdir -p db_assets/security
	python3 $<
	touch $@
	ls db_assets/security/

.env: .env.template
	cp $< $@

up: .env db_assets/security/.complete
	docker compose up --build -d

down:
	docker compose down

down-with-volumes:
	docker compose down --volumes
