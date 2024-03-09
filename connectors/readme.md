# Advantages of Using the Connector Factory
1. Decoupling: The factory pattern decouples the creation of objects from their usage. This means that the rest of your code doesn't need to know about the specific details of how connectors are instantiated.

2. Extensibility: Adding a new connector type is straightforward. You simply create a new connector class and update the ConnectorFactory's connectors dictionary. No other parts of your code need to change.

3. Centralization of Creation Logic: All logic related to creating connector instances is centralized in the factory. This makes maintenance easier and reduces duplication.

4. Flexibility: The factory makes it easy to instantiate connectors dynamically based on runtime conditions or configuration files.

This approach significantly enhances the maintainability and scalability of your ETL framework, allowing you to easily extend functionality with new types of connectors as your data sources evolve.

Each connector abstracts the complexity of connecting to different data sources, allowing you to focus on processing the data with PySpark. This modular approach makes it easier to extend the framework with new connectors as needed.
1. FileConnector
2. SFTPConnector
3. DatabaseConnector
4. APIConnector