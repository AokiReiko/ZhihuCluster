#!/bin/bash

set -x

hadoop com.sun.tools.javac.Main TrainingSetPrep.java
jar cf TrainingSetPrep.jar TrainingSetPrep*.class
