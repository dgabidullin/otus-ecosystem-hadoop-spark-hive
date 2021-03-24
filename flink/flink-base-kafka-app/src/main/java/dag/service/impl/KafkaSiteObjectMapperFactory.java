package dag.service.impl;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.time.Instant;

public class KafkaSiteObjectMapperFactory extends Module {

    public static ObjectMapper createObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);

        objectMapper.registerModule(new InstantModule());

        objectMapper.setVisibility(new VisibilityChecker.Std(
                JsonAutoDetect.Visibility.NONE,
                JsonAutoDetect.Visibility.NONE,
                JsonAutoDetect.Visibility.NONE,
                JsonAutoDetect.Visibility.NONE,
                JsonAutoDetect.Visibility.ANY
        ));
        return objectMapper;
    }

    @Override
    public String getModuleName() {
        return null;
    }

    @Override
    public Version version() {
        return null;
    }

    @Override
    public void setupModule(SetupContext setupContext) {

    }

    private static class InstantModule extends SimpleModule {

        InstantModule() {
            addSerializer(Instant.class, new InstantTimeSerializer());
            addDeserializer(Instant.class, new InstantTimeDeserializer());
        }

        public static class InstantTimeSerializer extends StdSerializer<Instant> {

            InstantTimeSerializer() {
                super(Instant.class);
            }

            @Override
            public void serialize(Instant instant, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
                jsonGenerator.writeNumber(instant.toEpochMilli());
            }
        }

        public static class InstantTimeDeserializer extends StdDeserializer<Instant> {

            InstantTimeDeserializer() {
                super(Instant.class);
            }

            @Override
            public Instant deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
                long value = jsonParser.getValueAsLong();
                return Instant.ofEpochMilli(value);
            }
        }
    }
}
