package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface IDUUIFolderPickerApi {

    public static class DUUIFolder {

        String id;
        String name;
        List<DUUIFolder> children;

        public DUUIFolder(String id, String name) {
            this.id = id;
            this.name = name;
            this.children = new ArrayList<>();
        }

        public void addChild(DUUIFolder child) {
            children.add(child);
        }

        public Map<String, Object> toJson() {
            Map<String, Object> map = new HashMap<>();

            map.put("id", id);
            map.put("content", name);
            map.put("children", children.stream().map(DUUIFolder::toJson).collect(Collectors.toList()));

            return map;
        }
    }

    DUUIFolder getFolderStructure();

}
