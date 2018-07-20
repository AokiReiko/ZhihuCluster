#!/bin/bash

set -x

hadoop com.sun.tools.javac.Main UserInfoPrep.java
jar cf UserInfoPrep.jar UserInfoPrep*.class
