pipeline {
  agent any
  stages {
    stage('setup_env') {
      steps {
        sh '''docker pull fpin/docker-hive-spark
mkdir data
# install conda and other deps

wget http://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
chmod +x miniconda.sh
./miniconda.sh -b
'''
        sh '''source /home/travis/miniconda3/bin/activate
conda install --yes -c conda-forge distributed pytest sasl thrift_sasl pandas python-snappy fastparquet pyarrow
pip install dockerctx pyhive'''
      }
    }
    stage('Run tests') {
      steps {
        sh '''source /home/travis/miniconda3/bin/activate
pytest -vrsx -s .'''
      }
    }
  }
}