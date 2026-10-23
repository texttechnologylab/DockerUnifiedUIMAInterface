package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaValue;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Lua JSON library ({@code lua_stdlib/json.lua}) with characters outside the Basic Multilingual Plane.
 *
 * <p>LuaJ stores a Java string as one UTF-8 sequence per UTF-16 code unit (CESU-8). Before the fix, json.encode wrote
 * these bytes unchanged, so a component received invalid UTF-8 (Python: two lone surrogates; e.g. the syntok
 * component then fell back to one paragraph for the whole text), and json.decode turned a JSON surrogate pair escape
 * into 4-byte UTF-8, which LuaJ cannot convert back to a Java string.
 */
public class TestLuaJsonNonBmp {

    // U+10341 GOTHIC LETTER NINETY, a surrogate pair in Java
    private static final String TEXT = "Absatz mit 𐍁 hier.\n\nZweiter Absatz: äß€";

    private static DUUILuaCompiledFile compile() throws Exception {
        return new DUUILuaContext().withJsonLibrary().compileFile(
                "function enc(s) return json.encode({text = s}) end\n"
                        + "function dec(s) return json.decode(s).text end\n");
    }

    private static byte[] bytes(LuaValue value) {
        LuaString s = value.checkstring();
        return Arrays.copyOfRange(s.m_bytes, s.m_offset, s.m_offset + s.m_length);
    }

    private static String strictUtf8(byte[] bytes) throws CharacterCodingException {
        return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes)).toString();
    }

    @Test
    public void encodeWritesValidUtf8WithSurrogateEscapes() throws Exception {
        byte[] json = bytes(compile().getGlobal("enc").call(LuaValue.valueOf(TEXT)));

        // valid UTF-8 (throws on the former CESU-8 bytes), the pair as JSON escapes, BMP characters unchanged
        String decoded = strictUtf8(json);
        assertTrue(decoded.contains("mit \\ud800\\udf41 hier"), decoded);
        assertTrue(decoded.contains("äß€"), decoded);
        // any JSON parser gets the original text back
        assertEquals(TEXT, new ObjectMapper().readTree(json).get("text").asText());
    }

    @Test
    public void decodeReturnsTheJavaStringOfAnEscapedPair() throws Exception {
        LuaValue text = compile().getGlobal("dec").call(LuaValue.valueOf("{\"text\": \"mit \\ud800\\udf41 hier\"}"));
        assertEquals("mit 𐍁 hier", text.tojstring());
    }

    @Test
    public void roundTrip() throws Exception {
        DUUILuaCompiledFile lua = compile();
        LuaValue json = lua.getGlobal("enc").call(LuaValue.valueOf(TEXT));
        assertEquals(TEXT, lua.getGlobal("dec").call(json).tojstring());
    }

    @Test
    public void textWithoutSurrogatesIsUnchanged() throws Exception {
        String bmp = "Ohne Sonderzeichen außer äß€ und \"Anführungszeichen\"";
        byte[] json = bytes(compile().getGlobal("enc").call(LuaValue.valueOf(bmp)));
        String expected = "{\"text\":\"Ohne Sonderzeichen außer äß€ und \\\"Anführungszeichen\\\"\"}";
        assertArrayEquals(expected.getBytes(StandardCharsets.UTF_8), json);
    }
}
