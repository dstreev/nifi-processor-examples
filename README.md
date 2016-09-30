# nifi-processor-examples
How to construct, test, build and deploy a custom Nifi Processor

# Step One - Building the Processor Project

Building out the Maven Project for a Nifi Processor.  There is a maven archetype that can be used to stub out your first Nifi processor.  Check the [Nifi Maven Archetype Documentation](https://cwiki.apache.org/confluence/display/NIFI/Maven+Projects+for+Extensions) for additional information.

```
➜  nifi-processor-examples git:(master) mvn archetype:generate -DarchetypeGroupId=org.apache.nifi -DarchetypeArtifactId=nifi-processor-bundle-archetype -DarchetypeVersion=1.0.0 -DnifiVersion=1.0.0
[INFO] Scanning for projects...
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building Maven Stub Project (No POM) 1
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] >>> maven-archetype-plugin:2.2:generate (default-cli) > generate-sources @ standalone-pom >>>
[INFO] Using property: nifiVersion = 1.0.0
Confirm properties configuration:
groupId: com.streever.iot.nifi
artifactId: nifi.processor.examples
version: 1.0-SNAPSHOT
artifactBaseName: examples
package: com.streever.iot.nifi.processors.examples
nifiVersion: 1.0.0
 Y: : y
[INFO] ----------------------------------------------------------------------------
[INFO] Using following parameters for creating project from Archetype: nifi-processor-bundle-archetype:1.0.0
[INFO] ----------------------------------------------------------------------------
[INFO] Parameter: groupId, Value: com.streever.iot.nifi
[INFO] Parameter: artifactId, Value: nifi.processor.examples
[INFO] Parameter: version, Value: 1.0-SNAPSHOT
[INFO] Parameter: package, Value: com.streever.iot.nifi.processors.examples
[INFO] Parameter: packageInPathFormat, Value: com/streever/iot/nifi/processors/examples
[INFO] Parameter: package, Value: com.streever.iot.nifi.processors.examples
[INFO] Parameter: artifactBaseName, Value: examples
[INFO] Parameter: version, Value: 1.0-SNAPSHOT
[INFO] Parameter: groupId, Value: com.streever.iot.nifi
[INFO] Parameter: artifactId, Value: nifi.processor.examples
[INFO] Parameter: nifiVersion, Value: 1.0.0
[INFO] project created from Archetype in dir: /Users/dstreev/projects/nifi-processor-examples/nifi.processor.examples
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 34.227 s
[INFO] Finished at: 2016-09-30T08:36:41-04:00
[INFO] Final Memory: 15M/309M
[INFO] ------------------------------------------------------------------------
```

The resulting tree structure should be similar to:
```
.
├── LICENSE
├── README.md
└── nifi.processor.examples
    ├── nifi-examples-nar
    │   └── pom.xml
    ├── nifi-examples-processors
    │   ├── pom.xml
    │   └── src
    │       ├── main
    │       │   ├── java
    │       │   │   └── com
    │       │   │       └── streever
    │       │   │           └── iot
    │       │   │               └── nifi
    │       │   │                   └── processors
    │       │   │                       └── examples
    │       │   │                           └── MyProcessor.java
    │       │   └── resources
    │       │       └── META-INF
    │       │           └── services
    │       │               └── org.apache.nifi.processor.Processor
    │       └── test
    │           └── java
    │               └── com
    │                   └── streever
    │                       └── iot
    │                           └── nifi
    │                               └── processors
    │                                   └── examples
    │                                       └── MyProcessorTest.java
    └── pom.xml

23 directories, 8 files
```


## Step #2 - Setup your IDE

My IDE of choice is Intellij.  So I simply create a new project, base it on existing sources and point to the master 'pom.xml' file generated from the step above.

Once the import is complete, your project should have a single "master" pom project and two maven sub-modules. There is a "nar" maven sub-module that will ultimately yield the artifact used to deploy to the "Nifi Server".  The other maven sub-module is where you build out the processor.  The parent "pom" inherits from the Nifi "Nar Bundles" maven project which contains all the details required to properly build the processor and package it.  It's very important that you don't change this parent dependency.  You won't see this project directly, it's pulled and added to you local maven repository during the first step.

## Step #3 - Building the Processor

The template that's constructed for you in the class "MyProcessor" is where all the "Nifi" magic happens.  But don't immediately jump into this class yet.

We need to build a component that accomplishes our tasks, independently from the processor.  Approaching it this way, allows us to build "components" that can be tested independently of the "Nifi" system.  In addition, allows our "business" logic to be used in other systems outside Nifi.

Generally, we'd have a completely separate project for the "business component", but for illustrative purposes and simplicity we'll build it here.  Although we will show how to approach testing the component in a Nifi independent manor.

The template created by the 'archetype' will also show how to test the component in the processor.

## Step #4 - Package and Deploy

Now that we've coded and tested the component and wrapped it in a processor, we need to create the Nifi "nar" package.

From the "Master POM" project, run:

```
mvn clean package
```

This will create a "nar" file in the "nar" sub-modules "target" directory.  Copy this file to HIFI_HOME/lib directory and restart Nifi.

The new processor is ready to use.  I've included a "template" for the processor we created here.

[Nifi File Part by RegEx Flow Template](./nifi-flow-templates/FilePartByRegEx.xml)

Happy Processoring!!

```