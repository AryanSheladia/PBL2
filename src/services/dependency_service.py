from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "password"

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))


def get_dependencies(section_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (s:Section {section_id: $section_id})
            OPTIONAL MATCH (s)-[:DEPENDS_ON*1..2]->(d)
            RETURN DISTINCT d.section_id AS dependent
            """,
            section_id=section_id,
        )

        return [record["dependent"] for record in result if record["dependent"]]