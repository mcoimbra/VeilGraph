from setuptools import setup


with open("README.txt", 'r') as f:
    long_description = f.read()


# https://stackoverflow.com/questions/1471994/what-is-setup-py
# https://github.com/pytorch/vision/blob/master/setup.py



setup(
   name='veilgraph',
   version='1.0',
   description='VeilGraph python utilities',
   long_description=long_description,
   author='Miguel Coimbra',
   license='Apache License, Version 2.0',
   author_email='miguel.e.coimbra@tecnico.ulisboa.pt',
   packages=['veilgraph'],  #same as name
   install_requires=['networkx', 'pathlib', 'psutil', 'pytz', 'matplotlib', 'numpy'], #external packages as dependencies
   scripts=[
            'algorithm/randomwalk/pagerank/run',
            'algorithm/randomwalk/pagerank/test',
            'dataset/sample_edges',
            'dataset/shuffle_edges',
            'dataset/duplicate_checker',
            'figure/randomwalk/pagerank/make_figures',
            'stream/streamer'
           ]
)