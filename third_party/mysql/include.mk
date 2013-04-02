MYSQL_VERSION := 5.1.24
MYSQL := third_party/mysql/mysql-connector-java-$(MYSQL_VERSION).jar
MYSQL_BASE_URL := http://repo1.maven.org/maven2/mysql/mysql-connector-java/$(MYSQL_VERSION)

$(MYSQL): $(MYSQL).md5
	set dummy "$(MYSQL_BASE_URL)" "$(MYSQL)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(MYSQL)
