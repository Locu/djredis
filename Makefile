.PHONY: clean

clean:
	find . -type f -name '*.py[cod]' -delete
	find . -type f -name '*~' -delete
	find . -type f -name '*.rdb' -delete

test: clean
	python runtests.py

installdeps:
	sudo pip install -r requirements.txt