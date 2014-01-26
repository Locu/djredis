.PHONY: clean

clean:
	find . -type f -name '*.py[cod]' -delete
	find . -type f -name '*~' -delete

test: clean
	python runtests.py

installdeps:
	sudo pip install -r requirements.txt