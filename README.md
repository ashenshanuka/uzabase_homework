<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Data Processing Project README</title>
  <style>
    body {
      font-family: Arial, sans-serif;
      margin: 20px;
      line-height: 1.6;
    }
    pre {
      background-color: #f4f4f4;
      padding: 10px;
      overflow-x: auto;
    }
    code {
      background-color: #f4f4f4;
      padding: 2px 4px;
    }
  </style>
</head>
<body>
  <h1>Data Processing Project</h1>
  <p>This project calculates the frequency of all unique words in the "description" column from news data. Two processing modes are available, each producing output in Parquet format. Log files are stored in the <code>logs</code> folder.</p>
  <p><strong>Features:</strong> Compute word frequency for news descriptions; two processing modes (<code>process_data</code> and <code>process_data_all</code>); output files saved as Parquet files in <code>ztmp/data/</code>; log files stored in the <code>logs</code> folder; Docker support for containerized execution; unit tests available via <code>pytest</code>.</p>
  <p><strong>Prerequisites:</strong> Python 3.8 or later, Docker, Git, and Pip.</p>
  <p><strong>Setup:</strong>  Install the required packages using <code>pip install -r requirements.txt</code>.</p>
  <p><strong>Running Data Processing Commands:</strong> To compute the word frequency for news descriptions, run:</p>
  <pre>
python src/run.py process_data -cfg config/cfg.yaml -dataset news -dirout "ztmp/data/"
  </pre>
  <p>To compute the word frequency using the alternative processing mode, run:</p>
  <pre>
python src/run.py process_data_all -cfg config/cfg.yaml -dataset news -dirout "ztmp/data/"
  </pre>
  <p>Output Parquet files will be saved in the <code>ztmp/data/</code> folder, and logs will be stored in the <code>logs</code> folder. A script is provided to execute both commands sequentially: <code>./script/run.sh</code>.</p>
  <p><strong>Running Tests:</strong> Execute unit tests with detailed logging by running:</p>
  <pre>
pytest -vv --log-level=DEBUG
  </pre>
  <p><strong>Docker Instructions:</strong> Build the Docker image using the command:</p>
  <pre>
docker build -f Dockerfile.Dockerfile -t uzabase_app:latest .
  </pre>
  <p>Then run the Docker container with:</p>
  <pre>
docker run --rm -v "$(pwd)":/app uzabase_app:latest
  </pre> 
  <p><strong>GitHub Actions:</strong> A GitHub Actions workflow is configured to build the Docker image on pushes and pull requests to the <code>main</code> branch. The workflow automatically builds and tags the Docker image using the specified Dockerfile.</p>

  <pre>

</html>
