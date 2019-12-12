# MDStudio workflow

[![Build Status](https://travis-ci.com/MD-Studio/MDStudio_workflow.svg?branch=master)](https://travis-ci.com/MD-Studio/MDStudio_workflow)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/423d531650af46b3a0909dc03246af66)](https://www.codacy.com/manual/marcvdijk/MDStudio_workflow?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=MD-Studio/MDStudio_workflow&amp;utm_campaign=Badge_Grade)
[![codecov](https://codecov.io/gh/MD-Studio/MDStudio_workflow/branch/master/graph/badge.svg)](https://codecov.io/gh/MD-Studio/MDStudio_workflow)

![Configuration settings](mdstudio-logo.png)

MDStudio workflow is a Python based workflow engine developed for the MDStudio microservice environment. MDtudio 
workfow makes it easy to construct complex workflows as DAGs (Directed Acyclic Graphs) combining calls to MDStudio
microservices with Python scripts or even commandline tools.
Examples on how to construct and use these workflows is provided in the [MDStudio_examples](https://github.com/MD-Studio/MDStudio_examples)
repository on GitHub.

## Installation Quickstart
MDStudio workflow can be used in the MDStudio environment as (Docker) service or as stand-alone Python library.

### Install option 1 Pre-compiled Docker container
MDStudio propka can be installed quickly from a pre-compiled docker image hosted on DockerHub by:

    docker pull mdstudio/mdstudio_workflow
    docker run (-d) mdstudio/mdstudio_workflow

In this mode you will first need to launch the MDStudio environment itself in order for the MDStudio workflow service to 
connect to it. You can unify this behaviour by adding the MDStudio workflow service to the MDStudio service environment as:

    MDStudio/docker-compose.yml:
        
        services:
           mdstudio_workflow:
              image: mdstudio/mdstudio_workflow
              links:
                - crossbar
              environment:
                - CROSSBAR_HOST=crossbar
              volumes:
                - ${WORKDIR}/mdstudio_workflow:/tmp/mdstudio/mdstudio_workflow

And optionally add `mdstudio_workflow` to MDStudio/core/auth/settings.dev.yml for automatic authentication and 
authorization at startup.

### Install option 2 custom build Docker container
You can custom build the MDStudio workflow Docker container by cloning the MDStudio_workflow GitHub repository and run:

    docker build MDStudio_workflow/ -t mdstudio/mdstudio_workflow
    
After successful build of the container follow the steps starting from `docker run` in install option 1.

### Install option 3 standalone deployment of the service
If you prefer a custom installation over a (pre-)build docker container you can clone the MDStudio_workflow GitHub
repository and install `mdstudio_workflow` locally as:

    pip install (-e) mdstudio_workflow/

Followed by:

    ./entry_point_mdstudio_workflow.sh
    
or

    export MD_CONFIG_ENVIRONMENTS=dev,docker
    python -u -m mdstudio_workflow

## Install option 4 use mdstudio_workflow as Python library
Install the project as standalone library as described in option 3. Build workflows as Python scripts explained in the
[MDStudio_examples](https://github.com/MD-Studio/MDStudio_examples) repository on GitHub.
