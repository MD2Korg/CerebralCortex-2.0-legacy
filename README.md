# CerebralCortex

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/b1d2febd95a74ade9ecb7bbc7e968292)](https://www.codacy.com/app/twhnat/CerebralCortex?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=MD2Korg/CerebralCortex&amp;utm_campaign=Badge_Grade)


## Intellij Setup
Environment Variables in Run Configuration

* PYTHONPATH=/Users/hnat/Downloads/spark-2.0.2-bin-hadoop2.7/python:/Users/hnat/Downloads/spark-2.0.2-bin-hadoop2.7/python/lib/py4j-0.10.3-src.zip
* SPARK_HOME=/Users/hnat/Downloads/spark-2.0.2-bin-hadoop2.7/


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