# Harvesters

RD-Switchboard Harvesters code. At this moment only OAI:PMH harvesting are supported and data source such as CrossRef will be harvested during Inference process. 

This Document contains only list of existsing projects in this repository. Please refer to the 
actual project README.md file for a information about how to install and use each tool. The project README.md 
located in each project folder.

# OAI:PMH Harvester

## [OAI:PMH Harvester](https://github.com/rd-switchboard/Harvesters/tree/master/OAI_PMH/harvester_oai)

Tool will harvest given OAI:PMH Repository and will store all data into S3 Bucket. The Harvested files will be stored into provides S3 Bucket as: `${provider.name}/${metadata.format}/${set.name}/${file.number}.xml`. If metadata format is not defined, the program will query OAI:PMH Provider and will print all possible metadata names. Program will store progres including resumption token into status file. The status file name will be generated as: `${provider.name}_${metadata.format}.xml`. If program has been resarted and status file exists in the local folder, the program will attempt to resume the harvesting process. Upon successfull harvesting, the status file will be deleted.

#### Requirements

Program requires Java 1.7 and Apache Maven 3.0.5

Program has been tested on Ubuntu Linux 14.04 and should work on any other linux as well

#### Build and ussage

To build the program simple run `mvn package` from the program folder.

For example:

```
cd OAI_PMH/harvester_oai
mvn package
```

The program archive will be avaliable in `OAI_PMH/harvester_oai/target/harvester_oai-${program_version}.tar.gz` (or bz2)

To install, copy the archive to desired location and untar it.

For example:

```
cd target
cp harvester_oai-1.3.0.tar.bz2 ~/
cd ~
tar -xjvf harvester_oai-1.3.0.tar.bz2
```

Before you can start the program, you must create and edit is's propertis file, located in `properties` folder. The sample file located at `properties/harvester.properties`

The possible properties are:

* url: OAI:PMH Provider URL. Can not be empty
* name: OAI:PMH Provider name. Can not be empty. We recommend to only use lower case letters and do not use spaces or any special symbolds, because this name will be used to generate file names.
* metadata: Name of metadata for harvesting. If empty, the harvester will only query OAI:PMH Provider for all possible metadata names, print them and then will stop. 
* aws.accessKey: Optional AWS Access Key. If none, the keys will be extracted from the Instance AIM Role.
* aws.secret.key: Optional AWS Secret Key. If none, the keys will be extracted from the Instance AIM Role.
* s3.bucket: S3 Bucket name

To stat the program, execute `java -jar harvester_oai-{program_version}.jar {properties_file}`. We recommed to run it as a background process and to use `nohup` keyword to protect the program from accidental termination.

For example:

```
# Switch into project directory
cd harvester_oai-1.3.0

# Edit program properties
cp properties/harvester.propertis properties/harvester_ands.propertis
vi properties/harvester_ands.propertis

# Execute the program
nohup java -jar harvester_oai-1.3.0.jar properties/harvester_ands.propertis >logs/harvester_ands.txt 2>&1 &
```

