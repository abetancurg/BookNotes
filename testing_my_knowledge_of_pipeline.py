# Copyright 2.022 Andres B.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
from pprint import pprint
import argparse

def main():
    parser = argparse.ArgumentParser(description="Testing my knowledge of pipeline")
    entrada = parser.add_argument("--entrada",help="Datos de entrada")
    salida = parser.add_argument("--salida",help="Datos de salida")
    num_registros = parser.add_argument("--nro-registros",help="Entrega el numero de registros")

    custom_args, beam_args = parser.parse_known_args()
    
    opts = PipelineOptions(beam_args)

    run_pipeline(custom_args,opts)

def run_pipeline(custom_args,opts):
    entrada = custom_args.entrada
    salida = custom_args.salida
    num_registros = custom_args.nro_registros

    with beam.Pipeline(options=opts) as p:
        linea = p | "Lectura de txt" >> beam.io.ReadFromText(entrada)
        linea | "Escritura a csv" >> beam.io.WriteToText(salida)
        # linea | beam.Map(pprint)

        #Pending proof this pipeline throught DataFlow
        #I can support from the ISSUE#1 of my own github account.

if __name__ == '__main__':
    main()