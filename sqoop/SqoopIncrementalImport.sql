sqoop import --connect jdbc:mysql://0.0.0.0/oc  --username mysqlusr --password password \
--table PRODUCT \
--target-dir /user/oc/sqoop/mysql/product \
--as-avrodatafile \
--m 1

sqoop import --connect jdbc:mysql://0.0.0.0/oc  --username mysqlusr --password password \
--table PRODUCT \
--target-dir /user/oc/sqoop/mysql/product \
--as-avrodatafile \
--incremental append \
--check-column id \
--last-value 5 \
--m 1