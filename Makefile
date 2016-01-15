test:
	./winda.py --debug add ../data/D160113.CSV

ed:
	sqlite3 winda.db

clean:
	find . -type d -name __pycache__ -print0 |xargs --no-run-if-empty -0 rm -rf
	find . -type f -name \*.pyc -print0 |xargs --no-run-if-empty -0 rm 
	rm -rf dist
	rm -f winda.db

