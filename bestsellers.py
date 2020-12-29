from __future__ import absolute_import
import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os

# set GOOGLE_APPLICATION_CREDENTIALS environment variable in Python code to the path key.json file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/annapooraniks/gcpdataflow/gcpdataflowkey.json"

# defining custom arguments - To add your own options, use the add_argument() method 
# specify a description which appears when a user passes --help as a command-line argument, and a default value. 
class MyOptions(PipelineOptions):    
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--input',
                        help='Input for the pipeline',
                        dest='input',
                        required=False,
                        default='gs://gcpdataflow/bestsellers.csv')
    parser.add_argument('--output',
                        help='Output for the pipeline',
                        required=False,
                        default='gs://gcpdataflow/computedresult.txt')

# class to split a csv line by elements and return only the columns we are interested in 
class Split(beam.DoFn):
    def process(self, element):
        Name, Author, Rating, Reviews, Price, Year, Genre = element.split(",")
        return [{
            'Name': str(Name),
            'Rating': float(Rating),
            'Price': float(Price),
            'Genre':str(Genre)
        }]

#MyTransform() is a composite transform. 
#It is used to perform 
#1. extract rating 
#2.calculate average rating 
#3.format the results for fiction books, non fiction books and all books. 
#So I have nested all these 3 simple transforms into a composite transform.

class MyTransform(beam.PTransform):
  def expand(self, input_col):
    a = (
        input_col
                   | beam.ParDo(ExtractRating()) 
                   | "Grouping keys" >> beam.GroupByKey() 
                   | "Calculating mean" >> beam.CombineValues(beam.combiners.MeanCombineFn())
                   | 'Apply Formatting' >> beam.Map(FormatText)
    )
    return a

# Returns a list of tuples containing  1 (key) and Rating value. 
# This form the input to GroupByKey, which takes (key,value) pair as input
class ExtractRating(beam.DoFn):
    def process(self, element):
        result = [(1, element['Rating'])]
        return result

# Returns a list of tuples containing the 1 (key) and Name value
# This form the input to GroupByKey, which takes (key,value) pair as input
class ExtractName(beam.DoFn):
    def process(self, element):
        result = [(1, element['Name'])]
        return result

#Function to filter elements based on the GenreName applied
def FilterBasedonGenre(GenreName,element):
  return element['Genre']==GenreName

#Function to format the output in a more readable way
def FormatText(elem):
  return 'AVERAGE RATING OF BOOKS:'+str(elem[1])

# setting input and output files
input_filename = "gs://gcpdataflow/bestsellers.csv"
output_filename = "gs://gcpdataflow/computedresult.txt"

# instantiate the pipeline
options = PipelineOptions()

with beam.Pipeline(options=options) as p:
    # read the contents of the file - bestsellers.csv in bestsellers PCollection 
    #and splitting lines by elements we want to retain
    bestsellers = (
            p | beam.io.ReadFromText(input_filename, skip_header_lines=1)
              | beam.ParDo(Split()) 
        )
    #creating a new pipeline to filter only the "Fiction" books using "Genre" column 
    #and then calculating the average user rating for all fiction books using MyTransform() 
    #and then printing the result in a file
    Fiction_pipeline = (
            bestsellers
            | beam.Filter(lambda record: FilterBasedonGenre('Fiction',record))
            | "Composite Transformation for Fiction elements" >> MyTransform()
            | "Write to Fiction_Results" >>beam.io.WriteToText('gs://gcpdataflow/Fiction_Result')
        )
    #creating a new pipeline to filter only the "Non Fiction" books using "Genre" column 
    #and then calculating the average user rating for all non fiction books using MyTransform() 
    #and then printing the result in a file

    NonFiction_pipeline = (
            bestsellers
            | beam.Filter(lambda record: FilterBasedonGenre('Non Fiction',record))
            | "Composite Transformation for Non Fiction" >> MyTransform()
            | "Write to NonFiction_Results" >>beam.io.WriteToText('gs://gcpdataflow/NonFiction_Result')
        )
        
    #creating a new pipeline for calculating the average user rating for all books using MyTransform() 
    #and then printing the result in a file
    AllBooks_pipeline = (
            bestsellers
            | "Composite Transformation for All Books" >> MyTransform()
            | "Write to Result file" >>beam.io.WriteToText(output_filename)
        )

    #creating a new pipeline to extract the Name of all the books as key value pairs,   
    #remove the duplicates from the name column using beam.Distinct()
    #sort it alphabetically, defaults to False (largest/descending). 
    #to use it ascending use reverse=True,  # optional, 
    SortBookNamesDesc_pipeline = (
            bestsellers
            |beam.ParDo(ExtractName())
            |'Deduplicate the elements' >> beam.Distinct()
            |'Sort book names in descending order' >> beam.combiners.Top.Of(5)
            |beam.Map(print)

    )
