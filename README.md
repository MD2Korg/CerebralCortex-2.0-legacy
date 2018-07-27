# CerebralCortex-2.0-legacy

Cerebral Cortex is the big data cloud companion of mCerebrum designed to support population-scale data analysis, visualization, model development, and intervention design for mobile sensor data.

Cerebral Cortex is the backend architecture, powered by Apache Spark, that is designed to analyze population-scale mHealth data

You can find more information about MD2K software on our [software website](https://md2k.org/software) or the MD2K organization on our [MD2K website](https://md2k.org/).

## Examples

#### Intellij Setup

Environment Variables in Run Configuration

* `PYTHONPATH=/Users/hnat/Downloads/spark-2.0.2-bin-hadoop2.7/python:/Users/hnat/Downloads/spark-2.0.2-bin-hadoop2.7/python/lib/py4j-0.10.3-src.zip`
* `SPARK_HOME=/Users/hnat/Downloads/spark-2.0.2-bin-hadoop2.7/`
* `PYSPARK_SUBMIT_ARGS=--packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 pyspark-shell`


Add `pyspark.zip` to your project libraries:

[http://stackoverflow.com/questions/13994846/intellij-python-plugin-run-classpath](http://stackoverflow.com/questions/13994846/intellij-python-plugin-run-classpath)
*  Go to File -> Project Structure
*  Now select Modules and then "Dependencies" tab
*  Click the "+" icon and select "Library"
*  Click "New Library" and select Java (I know it's weird...)
*  Now choose multiple modules / egg and "OK".
*  Select "Classes" from categories.
*  Give your new library a name, "My Python not Java Library"
*  And finally click "Add Selected"

## Contributing
Please read our [Contributing Guidelines](https://md2k.org/contributing/contributing-guidelines.html) for details on the process for submitting pull requests to us.

We use the [Python PEP 8 Style Guide](https://www.python.org/dev/peps/pep-0008/).

Our [Code of Conduct](https://md2k.org/contributing/code-of-conduct.html) is the [Contributor Covenant](https://www.contributor-covenant.org/).

Bug reports can be submitted through [JIRA](https://md2korg.atlassian.net/secure/Dashboard.jspa).

Our discussion forum can be found [here](https://discuss.md2k.org/).

## Versioning

We use [Semantic Versioning](https://semver.org/) for versioning the software which is based on the following guidelines.

MAJOR.MINOR.PATCH (example: 3.0.12)

  1. MAJOR version when incompatible API changes are made,
  2. MINOR version when functionality is added in a backwards-compatible manner, and
  3. PATCH version when backwards-compatible bug fixes are introduced.

For the versions available, see [this repository's tags](https://github.com/MD2Korg/CerebralCortex-2.0-legacy/tags).

## Contributors

Link to the [list of contributors](https://github.com/MD2Korg/CerebralCortex-2.0-legacy/graphs/contributors) who participated in this project.

## License

This project is licensed under the BSD 2-Clause - see the [license](https://md2k.org/software-under-the-hood/software-uth-license) file for details.

## Acknowledgments

* [National Institutes of Health](https://www.nih.gov/) - [Big Data to Knowledge Initiative](https://datascience.nih.gov/bd2k)
  * Grants: R01MD010362, 1UG1DA04030901, 1U54EB020404, 1R01CA190329, 1R01DE02524, R00MD010468, 3UH2DA041713, 10555SC
* [National Science Foundation](https://www.nsf.gov/)
  * Grants: 1640813, 1722646
* [Intelligence Advanced Research Projects Activity](https://www.iarpa.gov/)
  * Contract: 2017-17042800006

