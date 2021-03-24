package dag.service.impl;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;

class KafkaSiteJacksonModule extends SimpleModule {

    KafkaSiteJacksonModule() {
        super();
        addKeySerializer(String.class, new JsonSerializer<String>() {
            final PropertyNamingStrategy.SnakeCaseStrategy serializer = new PropertyNamingStrategy.SnakeCaseStrategy();

            @Override
            public void serialize(String s, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
                jsonGenerator.writeFieldName(serializer.translate(s));
            }
        });
    }
}
