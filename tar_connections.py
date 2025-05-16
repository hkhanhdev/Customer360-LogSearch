T_USERNAME = "root"
T_PASSWORD = ""
T_HOSTNAME = "192.168.0.101"
T_PORT = "3306"
T_SCHEMA_NAME = "class7"

T_JDBC_URL = f"jdbc:mysql://{T_HOSTNAME}:{T_PORT}/{T_SCHEMA_NAME}"
T_CONN_PROPERTIES = {
    "user": T_USERNAME,
    "password": T_PASSWORD,
    "driver": "com.mysql.cj.jdbc.Driver",
    "connectionPoolMaxSize" : "10",
    "batchsize": "10000",
    "autoCommit": "true"
}