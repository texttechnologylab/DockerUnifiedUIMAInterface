package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml.elements;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.annotation.ocr.abbyy.Block;
import org.xml.sax.Attributes;

public class AbbyyBlock extends AbstractStructuralAnnotation {
    private enum BlockTypeEnum {
        Text, Table, Picture, Barcode, Separator, SeparatorsBox, INVALID
    }

    private final BlockTypeEnum blockType;
    private final String blockName;


    public AbbyyBlock(Attributes attributes) {
        super(attributes);

        BlockTypeEnum blockType = BlockTypeEnum.INVALID;
        try {
            blockType = BlockTypeEnum.valueOf(attributes.getValue("blockType"));
        } catch (IllegalArgumentException invalid) {
            getLogger().warn("Unknown block type {}", blockType);
        }
        this.blockType = blockType;
        this.blockName = attributes.getValue("blockName");
    }

    @Override
    public Block into(JCas jcas, int start, int end) {
        Block block = new Block(jcas, start, end);
        block.setTop(top);
        block.setBottom(bottom);
        block.setLeft(left);
        block.setRight(right);
        block.setBlockType(blockType.name());
        block.setBlockName(blockName);
        return block;
    }

    public boolean isText() {
        return blockType == BlockTypeEnum.Text;
    }
}
