package com.zenfractal;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * For now, Spark does not support Avro. This class is just a quick
 * workaround that (de)serializes AminoAcid objects using Avro.
 */
public class SerializableAminoAcid extends AminoAcid implements Serializable {

    private void setValues(AminoAcid acid) {
        setAbbreviation(acid.getAbbreviation());
        setFullName(acid.getFullName());
        setMolecularWeight(acid.getMolecularWeight());
        setType(acid.getType());
    }

    public SerializableAminoAcid(AminoAcid acid) {
        setValues(acid);
    }

    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException {
        DatumWriter<AminoAcid> writer = new SpecificDatumWriter<AminoAcid>(AminoAcid.class);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(this, encoder);
        encoder.flush();
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        DatumReader<AminoAcid> reader =
                new SpecificDatumReader<AminoAcid>(AminoAcid.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        setValues(reader.read(null, decoder));
    }

    private void readObjectNoData()
            throws ObjectStreamException {
    }

}
