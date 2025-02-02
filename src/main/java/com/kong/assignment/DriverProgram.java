package com.kong.assignment;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DriverProgram {

    private final IngestProducer ingestProducer;
    private final IngestConsumer ingestConsumer;

    @Autowired
    public DriverProgram(IngestProducer ingestProducer, IngestConsumer ingestConsumer) {
        this.ingestProducer = ingestProducer;
        this.ingestConsumer = ingestConsumer;
    }

    @PostConstruct
    public void run() {
        ingestProducer.readJsonFile();
        ingestConsumer.writeToOpenSearch();
    }
}
