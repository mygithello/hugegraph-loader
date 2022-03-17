schema.propertyKey("ID").asText().ifNotExist().create();
schema.propertyKey("姓名").asText().ifNotExist().create();
schema.propertyKey("性别").asText().ifNotExist().create();
schema.propertyKey("备注").asText().ifNotExist().create();
schema.propertyKey("名称").asText().ifNotExist().create();
schema.vertexLabel("人物").properties("ID","姓名", "性别", "备注").primaryKeys("ID").ifNotExist().create();
schema.vertexLabel("根源").properties("ID","名称", "备注").primaryKeys("ID").ifNotExist().create();

schema.edgeLabel("夫妻").sourceLabel("人物").targetLabel("人物").ifNotExist().create();
schema.edgeLabel("子女").sourceLabel("人物").targetLabel("人物").ifNotExist().create();
schema.edgeLabel("小妾").sourceLabel("人物").targetLabel("人物").ifNotExist().create();
schema.edgeLabel("丫鬟").sourceLabel("人物").targetLabel("人物").ifNotExist().create();
schema.edgeLabel("继承").sourceLabel("根源").targetLabel("人物").ifNotExist().create();