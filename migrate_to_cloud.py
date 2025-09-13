import os
import pymongo
import psycopg2
import pandas as pd
from datetime import datetime
import logging
from typing import Optional
import sys
from dotenv import load_dotenv
load_dotenv()

# Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CloudMigrator:
    """Gestionnaire de migration vers les services cloud"""
    
    def __init__(self):
        self.local_mongo_client = None
        self.cloud_mongo_client = None
        self.local_pg_conn = None
        self.cloud_pg_conn = None
        
    def connect_local_mongodb(self):
        """Connexion à MongoDB local"""
        try:
            mongo_user = os.getenv("MONGO_USERNAME", "admin")
            mongo_pass = os.getenv("MONGO_PASSWORD", "")
            
            connection_string = f"mongodb://{mongo_user}:{mongo_pass}@localhost:27018/"
            self.local_mongo_client = pymongo.MongoClient(connection_string)
            
            # Test de connexion
            self.local_mongo_client.admin.command('ping')
            logger.info("✅ Connexion MongoDB local réussie")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur connexion MongoDB local: {e}")
            return False
    
    def connect_cloud_mongodb(self, connection_string: str):
        """Connexion à MongoDB Atlas"""
        try:
            self.cloud_mongo_client = pymongo.MongoClient(connection_string)
            
            # Test de connexion
            self.cloud_mongo_client.admin.command('ping')
            logger.info("✅ Connexion MongoDB Atlas réussie")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur connexion MongoDB Atlas: {e}")
            return False
    
    def connect_local_postgresql(self):
        """Connexion à PostgreSQL local"""
        try:
            self.local_pg_conn = psycopg2.connect(
                host="localhost",
                port=5432,
                database="prefect",
                user="prefect",
                password="prefect"
            )
            logger.info("✅ Connexion PostgreSQL local réussie")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur connexion PostgreSQL local: {e}")
            return False
    
    def connect_cloud_postgresql(self, connection_string: str):
        """Connexion à Neon PostgreSQL"""
        try:
            self.cloud_pg_conn = psycopg2.connect(connection_string)
            logger.info("✅ Connexion Neon PostgreSQL réussie")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur connexion Neon PostgreSQL: {e}")
            return False
    
    def migrate_mongodb_data(self, database_name: str = "youtube_analysis"):
        """Migration des données MongoDB"""
        if not self.local_mongo_client or not self.cloud_mongo_client:
            logger.error("❌ Connexions MongoDB non établies")
            return False
        
        try:
            # Base locale
            local_db = self.local_mongo_client[database_name]
            
            # Base cloud
            cloud_db = self.cloud_mongo_client[database_name]
            
            # Lister les collections
            collections = local_db.list_collection_names()
            logger.info(f"📋 Collections trouvées: {collections}")
            
            total_docs = 0
            for collection_name in collections:
                logger.info(f"🔄 Migration collection: {collection_name}")
                
                # Collection locale
                local_collection = local_db[collection_name]
                local_count = local_collection.count_documents({})
                
                if local_count == 0:
                    logger.info(f"⏭️ Collection {collection_name} vide, ignorée")
                    continue
                
                # Collection cloud
                cloud_collection = cloud_db[collection_name]
                
                # Vider la collection cloud si elle existe
                cloud_collection.delete_many({})
                
                # Migration par batch pour éviter les timeouts
                batch_size = 1000
                docs_migrated = 0
                
                for batch in local_collection.find().batch_size(batch_size):
                    batch_docs = []
                    current_batch = [batch]  # Premier document
                    
                    # Récupérer le batch complet
                    try:
                        for _ in range(batch_size - 1):
                            batch_docs.append(next(local_collection.find().skip(docs_migrated + len(current_batch))))
                            current_batch.extend(batch_docs[-1:])
                    except StopIteration:
                        pass  # Fin des documents
                    
                    if current_batch:
                        # Insérer le batch
                        cloud_collection.insert_many(current_batch)
                        docs_migrated += len(current_batch)
                        logger.info(f"   📄 {docs_migrated}/{local_count} documents migrés")
                
                total_docs += docs_migrated
                logger.info(f"✅ Collection {collection_name}: {docs_migrated} documents migrés")
            
            logger.info(f"🎉 Migration MongoDB terminée: {total_docs} documents au total")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur migration MongoDB: {e}")
            return False
    
    def migrate_postgresql_data(self):
        """Migration des données PostgreSQL (Prefect)"""
        if not self.local_pg_conn or not self.cloud_pg_conn:
            logger.error("❌ Connexions PostgreSQL non établies")
            return False
        
        try:
            local_cur = self.local_pg_conn.cursor()
            cloud_cur = self.cloud_pg_conn.cursor()
            
            # Lister les tables Prefect
            local_cur.execute("""
                SELECT tablename FROM pg_tables 
                WHERE schemaname = 'public' 
                AND tablename LIKE '%prefect%' OR tablename IN ('flow', 'flow_run', 'task_run');
            """)
            
            tables = [row[0] for row in local_cur.fetchall()]
            logger.info(f"📋 Tables Prefect trouvées: {tables}")
            
            if not tables:
                logger.warning("⚠️ Aucune table Prefect trouvée, migration des métadonnées seulement")
                return True
            
            for table_name in tables:
                logger.info(f"🔄 Migration table: {table_name}")
                
                # Compter les lignes
                local_cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = local_cur.fetchone()[0]
                
                if row_count == 0:
                    logger.info(f"⏭️ Table {table_name} vide, ignorée")
                    continue
                
                # Récupérer la structure de la table
                local_cur.execute(f"""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position;
                """)
                
                columns = local_cur.fetchall()
                
                # Créer la table sur le cloud (si elle n'existe pas)
                create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
                column_defs = []
                
                for col_name, data_type, is_nullable, col_default in columns:
                    col_def = f"{col_name} {data_type}"
                    if is_nullable == 'NO':
                        col_def += " NOT NULL"
                    if col_default:
                        col_def += f" DEFAULT {col_default}"
                    column_defs.append(col_def)
                
                create_table_sql += ", ".join(column_defs) + ");"
                
                try:
                    cloud_cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
                    cloud_cur.execute(create_table_sql)
                    self.cloud_pg_conn.commit()
                except Exception as e:
                    logger.warning(f"⚠️ Erreur création table {table_name}: {e}")
                    continue
                
                # Migration des données
                local_cur.execute(f"SELECT * FROM {table_name}")
                rows = local_cur.fetchall()
                
                if rows:
                    # Préparer l'insertion
                    col_names = [col[0] for col in columns]
                    placeholders = ", ".join(["%s"] * len(col_names))
                    insert_sql = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({placeholders})"
                    
                    cloud_cur.executemany(insert_sql, rows)
                    self.cloud_pg_conn.commit()
                
                logger.info(f"✅ Table {table_name}: {len(rows)} lignes migrées")
            
            logger.info("🎉 Migration PostgreSQL terminée")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur migration PostgreSQL: {e}")
            return False
        finally:
            if 'local_cur' in locals():
                local_cur.close()
            if 'cloud_cur' in locals():
                cloud_cur.close()
    
    def close_connections(self):
        """Fermer toutes les connexions"""
        if self.local_mongo_client:
            self.local_mongo_client.close()
        if self.cloud_mongo_client:
            self.cloud_mongo_client.close()
        if self.local_pg_conn:
            self.local_pg_conn.close()
        if self.cloud_pg_conn:
            self.cloud_pg_conn.close()

def main():
    """Migration principale"""
    print("🚀 Migration des données locales vers le cloud")
    print("=" * 50)
    
    # Vérifier les variables d'environnement
    required_vars = [
        "CONNECTING_STRING_ATLAS",  # Atlas
        "CONNECTING_STRING_NEON"  # Neon
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"❌ Variables d'environnement manquantes: {missing_vars}")
        logger.info("💡 Configure-les avec:")
        logger.info("export CONNECTING_STRING_ATLAS='mongodb+srv://...'")
        logger.info("export CONNECTING_STRING_NEON='postgresql://...'")
        sys.exit(1)
    
    migrator = CloudMigrator()
    
    try:
        # Connexions locales
        mongo_local_ok = migrator.connect_local_mongodb()
        pg_local_ok = migrator.connect_local_postgresql()
        
        if not mongo_local_ok:
            logger.warning("⚠️ MongoDB local non accessible, migration ignorée")
        if not pg_local_ok:
            logger.warning("⚠️ PostgreSQL local non accessible, migration ignorée")
        
        # Connexions cloud
        mongo_cloud_ok = migrator.connect_cloud_mongodb(
            os.getenv("CONNECTING_STRING_ATLAS")
        )
        pg_cloud_ok = migrator.connect_cloud_postgresql(
            os.getenv("CONNECTING_STRING_NEON")
        )
        
        if not mongo_cloud_ok:
            logger.error("❌ Impossible de se connecter à MongoDB Atlas")
            return False
        if not pg_cloud_ok:
            logger.error("❌ Impossible de se connecter à Neon PostgreSQL")
            return False
        
        # Migrations
        success = True
        
        if mongo_local_ok and mongo_cloud_ok:
            logger.info("📦 Migration MongoDB en cours...")
            if not migrator.migrate_mongodb_data():
                success = False
        
        if pg_local_ok and pg_cloud_ok:
            logger.info("📦 Migration PostgreSQL en cours...")
            if not migrator.migrate_postgresql_data():
                success = False
        
        if success:
            logger.info("🎉 Migration terminée avec succès!")
            logger.info("📋 Prochaines étapes:")
            logger.info("1. Vérifier les données sur les services cloud")
            logger.info("2. Configurer les secrets sur Streamlit Cloud")
            logger.info("3. Déployer l'application")
        else:
            logger.error("❌ Migration terminée avec des erreurs")
        
        return success
        
    finally:
        migrator.close_connections()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)