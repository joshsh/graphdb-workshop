#!/bin/bash

# find Java
if [ "$JAVA_HOME" = "" ] ; then
	JAVA="java"
else
	JAVA="$JAVA_HOME/bin/java"
fi

# set Java options
if [ "$JAVA_OPTIONS" = "" ] ; then
	JAVA_OPTIONS="-Xms512m -Xmx1g"
fi

LINK=`readlink $0`
if [ "$LINK" ]; then
    DIR=`dirname $LINK`
else
    DIR=`dirname $0`
fi

exec $JAVA $JAVA_OPTIONS -cp $DIR/target/classes:$DIR/"target/dependency/*" com.tinkerpop.etc.github.GithubLoader $*
