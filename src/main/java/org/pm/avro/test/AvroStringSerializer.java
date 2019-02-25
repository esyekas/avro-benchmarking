
package org.pm.avro.test;

import example.avro.Message_string;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 *
 * @author pmaresca
 */
public class AvroStringSerializer
{
    private static AvroStringSerializer instance;
    
    private boolean inited;
    
    private DatumReader<Message_string> reader;
    private DatumWriter<Message_string> writer;
    
    private ByteArrayOutputStream baos;
    
    private AvroStringSerializer() { this.inited = false; }
    
    public synchronized static AvroStringSerializer instance()
    {
        if(instance == null)
            instance = new AvroStringSerializer();
        
        return instance;
    }
    
    public byte[] serialize(Message_string msg) throws IOException, IllegalStateException
    {
        if(!inited)
            throw new IllegalStateException("Serializer must be initialized");
        
        this.baos.reset();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(this.baos, null);
        
        
        writer.write(msg, encoder);
        encoder.flush();
        this.baos.flush();
        
        return this.baos.toByteArray();
    }
    
    public Object deserialize(byte[] msg) throws IOException, IllegalStateException
    { 
        if(!inited)
            throw new IllegalStateException("Serializer must be initialized");
        
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(msg, null);
        
        return reader.read(null, decoder);
    }
    
    public void setUp() throws IOException 
    {
        this.reader = new SpecificDatumReader<>(Message_string.getClassSchema());
        this.writer = new SpecificDatumWriter<>(Message_string.getClassSchema());
        this.baos = new ByteArrayOutputStream();
        this.inited = true;
    }
    
    public void tearDown() throws IOException 
    {
        this.reader = null;
        this.writer = null;
        this.baos.close();
        this.inited = false;
    }
    
}
