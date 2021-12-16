# DEPLOYMENT

This Readme contains the steps to run the Data Engineer Challenge Locally using docker

To deploy you need to have docker installed in your computer, after that you have to enter the following command

docker pull felipegarridof/fgarrido-de-challenge:final

With that you will download an image with the project, after this you need to execute the following command to run the docker image

docker run -v {INSERTDATAPATH}:/data-processed felipegarridof/fgarrido-de-challenge:final

you need to replace the value {INSERTDATAPATH} with the path in your local where you have the consoles.csv and result.csv data. Also you can replace it with ${PWD}, so it will run in your current location

Observations:

There is an issue in the reports, there is a empty column '' with ids that shouldn't be there, this problem was generated in the line 33 from the data_converter.py where I cast a Dataframe to a Pandas Dataframe, that cast generates that empty column with autoincremental integers. I noticed this too late and wasn't able to fix it. I tried to drop that column, rename it, just write the other colums but still apeared at the end. The solution that I wanted to apply was to work with pandas dataframes from the beggining.

As a final note, I switched midway the code to Pandas Dataframe because I was unable to cast the "date" column to date using "normal" Dataframe. 

# DATAMODEL

To create the datamodel, I worked with draw.io (https://app.diagrams.net/), I used this tool because is universally known and it's free

Also, I created this model but I didn't use it in the code itself, the way I aproached this was to merge everything in a single table to starting generating the reports