function Message(channel, content) {
        this.channel = channel;
        this.content = content;
        // this.signal = signal;
        this.clientId = "474ed958-c86d-49d8-859b-4b7f5d006127";
        this.clientSequenceNumber = "1";
        this.minimumSequenceNumber = 15;
        this.referenceSequenceNumber = 17;
        this.sequenceNumber = 18;
        this.term = 1;
        this.timestamp = Date.now();
        this.type = "summarize";
        this.extraContent = "{\"handle\":\"5fc9f449-f58b-4a9a-9ad1-d0e8c3a54474\",\"head\":\"d6745912-3571-4b3f-957f-367bbd622f44\",\"message\":\"Summary @17:15\",\"parents\":[\"d6745912-3571-4b3f-957f-367bbd622f44\"]}";
}

module.exports = Message;