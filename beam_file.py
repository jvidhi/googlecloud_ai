#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A word-counting workflow."""

# pytype: skip-file

# beam-playground:
#   name: WordCount
#   description: An example that counts words in Shakespeare's works.
#   multifile: false
#   pipeline_options: --output output.txt
#   context_line: 87
#   categories:
#     - Combiners
#     - Options
#     - Quickstart
#   complexity: MEDIUM
#   tags:
#     - options
#     - count
#     - combine
#     - strings

import apache_beam as beam
from apache_beam.ml.inference.vertex_ai_inference import VertexAIModelHandlerJSON
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.options.pipeline_options import PipelineOptions
from collections.abc import Iterable
import json
import google.cloud.aiplatform as aip

import ast

from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value



class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""
  def process(self, element):
    """Returns an iterator over the words of this element.

    The element is a line of text.  If the line is blank, note that, too.

    Args:
      element: the element being processed

    Returns:
      The processed element.
    """
    return re.findall(r'[\w\']+', element, re.UNICODE)


class Preprocess(beam.DoFn):
    def __init__(self):
        pass
    
    def process(self, message):
        message_list = [ast.literal_eval(message.decode("utf-8"))]
        return [json_format.ParseDict(instance_dict, Value()) for instance_dict in message_list]
        #return ["""{"instances":[{"user_pseudo_id": ["1BE4F29852B390FC94D2A4E7382CCEBD"]}] }"""]
        #return [example_json]
        # return """{"instances": [{}] }""".format(message)
        
class Postprocess(beam.DoFn):
    def __init__(self):
        pass
    
    def process(self, element):
        print("ENTER POSTPROCESS")
        input_example = element.example
        prediction_vals = element.inference
        
        print("input")
        print(input_example.struct_value["user_pseudo_id"])#.get("user_pseudo_id"))
        print("predictions")
        print(prediction_vals["predicted_churned"][0])
        print(max(prediction_vals["churned_probs"]))
        
        return [{"user_pseudo_id": input_example.struct_value["user_pseudo_id"], "churn": prediction_vals["predicted_churned"][0], "probability": max(prediction_vals["churned_probs"])}]
        # index = prediction_vals.index(max(prediction_vals))
        # yield str(COLUMNS[index]) + " (" + str(max(prediction_vals)) + ")"
        

def run(argv=None, save_main_session=True):
  """Runs the pipeline"""
#   parser = argparse.ArgumentParser()
#   parser.add_argument(
#       '--input',
#       dest='input',
#       default='gs://dataflow-samples/shakespeare/kinglear.txt',
#       help='Input file to process.')
#   parser.add_argument(
#       '--output',
#       dest='output',
#       required=True,
#       help='Output file to write results to.')
#   known_args, pipeline_args = parser.parse_known_args(argv)

#   # We use the save_main_session option because one or more DoFn's in this
#   # workflow rely on global context (e.g., a module imported at module level).
#   pipeline_options = PipelineOptions(pipeline_args)
#   pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  project = "vertex-ai-382806"
  location = "us-central1"
  aip.init(project=project, location=location)
  model_handler = VertexAIModelHandlerJSON(endpoint_id=endpoint_id, project=project, location=location)
  subscription = "projects/vertex-ai-382806/subscriptions/churn_prediction_topic-sub"
  bq_location = "vertex-ai-382806.cloud_summit_demo.predictions"
  # options=pipeline_options

  pipeline_options = PipelineOptions(streaming=True)

  with beam.Pipeline(options=pipeline_options) as p:
          elements = (  
              p  | "Read PubSub"  >> beam.io.ReadFromPubSub(
                  subscription=subscription
              ).with_output_types(bytes)
             #| "print1" >> beam.Map(print)
             #| "Create manual input" >> beam.Create(instances) 
             | "Preprocess" >> beam.ParDo(Preprocess())
             # | "print2" >> beam.Map(print)
              | "Run Vertex Inference" >> RunInference(model_handler)
              | "Process Output" >> beam.ParDo(Postprocess())
              #| "Write to BigQuery"
              #>> beam.io.WriteToBigQuery(
              #table=bq_location
              #)
              #| "print3" >> beam.Map(print)
          )


#   # The pipeline will be run on exiting the with block.
#   with beam.Pipeline(options=pipeline_options) as p:

#     # Read the text file[pattern] into a PCollection.
#     lines = p | 'Read' >> ReadFromText(known_args.input)

#     counts = (
#         lines
#         | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
#         | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
#         | 'GroupAndSum' >> beam.CombinePerKey(sum))

#     # Format the counts into a PCollection of strings.
#     def format_result(word, count):
#       return '%s: %d' % (word, count)

#     output = counts | 'Format' >> beam.MapTuple(format_result)

#     # Write the output using a "Write" transform that has side effects.
#     # pylint: disable=expression-not-assigned
#     output | 'Write' >> WriteToText(known_args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()