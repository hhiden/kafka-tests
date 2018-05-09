
package com.redhat.kafkatest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 *
 * @author hhiden
 */
public class SerializationUtils {
    public static final Object deserialize(byte[] data) throws Exception {
        try(ByteArrayInputStream stream = new ByteArrayInputStream(data)){
            try (ObjectInputStream obStream = new ObjectInputStream(stream)){
                return obStream.readObject();
            }
        }
    }
    
    public static final byte[] serialize(Object object) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try (ObjectOutputStream obStream = new ObjectOutputStream(stream)){
            obStream.writeObject(object);
        }
        stream.flush();
        stream.close();
        return stream.toByteArray();
    }
}
