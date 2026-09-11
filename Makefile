.PHONY: requirements

requirements:
	NO_COLOR=1 uv export --color never --format requirements-txt --no-dev > requirements.txt
