# Example code for franz-go package

This is a simple example of how to use the franz-go package to produce and consume messages.

# pkafka package

This is an abstraction on top of franz-go which provides a method that will produce on a kafka topic. if kafka isn't 
available it'll retry until the message is produced.
