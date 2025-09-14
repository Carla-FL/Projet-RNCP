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
            mongo_port = os.getenv("MONGO_PORT", "27018")

            connection_string = f"mongodb://{mongo_user}:{mongo_pass}@localhost:{mongo_port}/"
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
    
    def migrate_mongodb_data(self, database_name_cloud :str="youtube-analysis"): # database_name_local: str = "UCDkl5M0WVaddTWE4rr2cSeA"
        """Migration des données MongoDB"""
        if not self.local_mongo_client or not self.cloud_mongo_client:
            logger.error("❌ Connexions MongoDB non établies")
            return False
        
        try:
            # Détecter automatiquement la base locale si non spécifiée
            if not database_name_local:
                db_names = self.local_mongo_client.list_database_names()
                # Exclure les bases système
                user_dbs = [name for name in db_names if name not in ['admin', 'local', 'config']]
                if user_dbs:
                    database_name_local = user_dbs[0]  # Prendre la première base utilisateur
                    logger.info(f"🔍 Base locale détectée automatiquement: {database_name_local}")
                else:
                    logger.error("❌ Aucune base de données utilisateur trouvée")
                    return False

            # Base locale
            local_db = self.local_mongo_client[database_name_local]
            
            # Base cloud
            cloud_db = self.cloud_mongo_client[database_name_cloud]
            
            # Lister les collections
            collections = local_db.list_collection_names()
            logger.info(f"📋 Collections trouvées: {collections}")

            if not collections:
                logger.warning("⚠️ Aucune collection trouvée dans la base locale")
                return True
            
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
                
                #  Vider complètement la collection cloud
                deleted_count = cloud_collection.delete_many({}).deleted_count
                logger.info(f"🧹 {deleted_count} documents supprimés de {collection_name} sur Atlas")
                
                # Migration par batch pour éviter les timeouts
                batch_size = 500
                docs_migrated = 0

                # Récupérer tous les documents
                logger.info(f"📥 Récupération de {local_count} documents...")
                all_docs = list(local_collection.find())
                
                # Traiter par batch
                for i in range(0, len(all_docs), batch_size):
                    batch = all_docs[i:i + batch_size]
                    batch_num = i // batch_size + 1
                    total_batches = (len(all_docs) + batch_size - 1) // batch_size
                    
                    # Récupérer le batch complet
                    try:
                        # Insérer le batch (ordered=False pour continuer même si erreur)
                        result = cloud_collection.insert_many(batch, ordered=False)
                        docs_migrated += len(result.inserted_ids)
                        logger.info(f"   📄 Batch {batch_num}/{total_batches}: {len(result.inserted_ids)} documents insérés")
                        
                    except pymongo.errors.BulkWriteError as e:
                        # Gérer les erreurs bulk (ex: doublons)
                        inserted_count = e.details.get('nInserted', 0)
                        docs_migrated += inserted_count
                        logger.warning(f"⚠️ Batch {batch_num}/{total_batches}: {inserted_count} insérés, {len(e.details.get('writeErrors', []))} erreurs")
                        
                    except Exception as e:
                        logger.error(f"❌ Erreur batch {batch_num}: {e}")
                        # Essayer document par document pour ce batch
                        for doc_idx, doc in enumerate(batch):
                            try:
                                cloud_collection.insert_one(doc)
                                docs_migrated += 1
                            except:
                                logger.debug(f"Document {doc_idx} ignoré (probablement un doublon)")
                
                
                total_docs += docs_migrated
                logger.info(f"✅ Collection {collection_name}: {docs_migrated}/{local_count} documents migrés")
            
            logger.info(f"🎉 Migration MongoDB terminée: {total_docs} documents au total migrés vers '{database_name_cloud}'")
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
            cloud_cur = self.cloud_pg_conn.cursor()
            
            logger.info("🔧 Préparation de la base Neon pour Prefect...")
            
            # Test de la connexion et récupération des infos
            cloud_cur.execute("SELECT version();")
            version = cloud_cur.fetchone()[0]
            logger.info(f"📊 Version PostgreSQL Neon: {version}")
            
            # Créer le schéma public si nécessaire
            try:
                cloud_cur.execute("CREATE SCHEMA IF NOT EXISTS public;")
                self.cloud_pg_conn.commit()
                logger.info("✅ Schéma public vérifié sur Neon")
            except Exception as e:
                logger.debug(f"Schéma public existe déjà: {e}")
            
            # Créer une table de test pour vérifier les permissions
            try:
                cloud_cur.execute("""
                    CREATE TABLE IF NOT EXISTS migration_test (
                        id SERIAL PRIMARY KEY,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        status TEXT
                    );
                """)
                
                # Insérer un enregistrement de test
                cloud_cur.execute("""
                    INSERT INTO migration_test (status) 
                    VALUES ('Migration script executed successfully');
                """)
                
                self.cloud_pg_conn.commit()
                
                # Vérifier l'insertion
                cloud_cur.execute("SELECT COUNT(*) FROM migration_test;")
                count = cloud_cur.fetchone()[0]
                logger.info(f"✅ Test d'écriture réussi: {count} enregistrement(s) dans migration_test")
                
                # Nettoyer
                cloud_cur.execute("DROP TABLE migration_test;")
                self.cloud_pg_conn.commit()
                
            except Exception as e:
                logger.error(f"❌ Test d'écriture échoué: {e}")
                return False
            
            logger.info("✅ Base Neon PostgreSQL prête pour Prefect")
            logger.info("ℹ️ Les tables Prefect se créeront automatiquement au premier démarrage de l'orchestrateur")
            
            # Si on a une connexion locale, on peut essayer de migrer quelques métadonnées
            if self.local_pg_conn:
                logger.info("🔄 Tentative de migration des métadonnées Prefect...")
                try:
                    local_cur = self.local_pg_conn.cursor()
                    
                    # Vérifier s'il y a des données Prefect
                    local_cur.execute("""
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND (table_name LIKE '%prefect%' OR table_name IN ('flow', 'flow_run', 'task_run'));
                    """)
                    
                    table_count = local_cur.fetchone()[0]
                    if table_count > 0:
                        logger.info(f"📊 {table_count} tables Prefect trouvées localement")
                        logger.info("⚠️ Migration des données Prefect ignorée (compatibilité)")
                        logger.info("💡 Conseil: Redéploye tes flows Prefect après la migration")
                    else:
                        logger.info("ℹ️ Aucune table Prefect trouvée localement")
                        
                    local_cur.close()
                        
                except Exception as e:
                    logger.warning(f"⚠️ Vérification locale Prefect échouée: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur préparation PostgreSQL: {e}")
            return False
        finally:
            if 'cloud_cur' in locals():
                cloud_cur.close()
    
    def close_connections(self):
        """Fermer toutes les connexions"""
        connections_closed = 0
        
        if self.local_mongo_client:
            self.local_mongo_client.close()
            connections_closed += 1
            
        if self.cloud_mongo_client:
            self.cloud_mongo_client.close()
            connections_closed += 1
            
        if self.local_pg_conn:
            self.local_pg_conn.close()
            connections_closed += 1
            
        if self.cloud_pg_conn:
            self.cloud_pg_conn.close()
            connections_closed += 1
            
        logger.info(f"🔌 {connections_closed} connexions fermées")

def check_environment():
    """Vérifier les variables d'environnement"""
    required_vars = {
        "CONNECTING_STRING_ATLAS": "Connection string MongoDB Atlas",
        "CONNECTING_STRING_NEON": "Connection string Neon PostgreSQL"
    }
    
    missing_vars = []
    for var, description in required_vars.items():
        if not os.getenv(var):
            missing_vars.append(f"{var} ({description})")
    
    if missing_vars:
        logger.error("❌ Variables d'environnement manquantes:")
        for var in missing_vars:
            logger.error(f"   - {var}")
        logger.info("")
        logger.info("💡 Configure-les dans ton fichier .env ou en variables d'environnement:")
        logger.info("export CONNECTING_STRING_ATLAS='mongodb+srv://username:password@cluster.mongodb.net/'")
        logger.info("export CONNECTING_STRING_NEON='postgresql://username:password@hostname:5432/database'")
        return False
    
    return True

def main():
    """Migration principale"""
    print("🚀 Migration des données locales vers le cloud")
    print("=" * 60)
    
    # Vérifier l'environnement
    if not check_environment():
        sys.exit(1)
    
    migrator = CloudMigrator()
    success = True
    
    try:
        # Phase 1: Test des connexions locales
        logger.info("🔌 Phase 1: Test des connexions locales...")
        mongo_local_ok = migrator.connect_local_mongodb()
        pg_local_ok = migrator.connect_local_postgresql()
        
        if not mongo_local_ok and not pg_local_ok:
            logger.error("❌ Aucune connexion locale n'a fonctionné")
            logger.info("💡 Assure-toi que tes services locaux sont démarrés (docker-compose up)")
            return False
        
        # Phase 2: Test des connexions cloud
        logger.info("🌐 Phase 2: Test des connexions cloud...")
        
        # MongoDB Atlas
        mongo_cloud_ok = migrator.connect_cloud_mongodb(
            os.getenv("CONNECTING_STRING_ATLAS")
        )
        
        # Neon PostgreSQL
        pg_cloud_ok = migrator.connect_cloud_postgresql(
            os.getenv("CONNECTING_STRING_NEON")
        )
        
        if not mongo_cloud_ok and not pg_cloud_ok:
            logger.error("❌ Aucune connexion cloud n'a fonctionné")
            logger.info("💡 Vérifie tes connection strings et la configuration réseau")
            return False
        
        # Phase 3: Migrations
        logger.info("📦 Phase 3: Migration des données...")
        
        # Migration MongoDB
        if mongo_local_ok and mongo_cloud_ok:
            logger.info("🍃 Migration MongoDB en cours...")
            if not migrator.migrate_mongodb_data():
                logger.warning("⚠️ Migration MongoDB échouée")
                success = False
            else:
                logger.info("✅ Migration MongoDB réussie")
        else:
            logger.warning("⏭️ Migration MongoDB ignorée (connexions manquantes)")
        
        # Migration PostgreSQL
        if pg_cloud_ok:
            logger.info("🐘 Préparation PostgreSQL en cours...")
            if not migrator.migrate_postgresql_data():
                logger.warning("⚠️ Préparation PostgreSQL échouée")
                success = False
            else:
                logger.info("✅ Préparation PostgreSQL réussie")
        else:
            logger.warning("⏭️ Préparation PostgreSQL ignorée (connexion cloud manquante)")
        
        # Résultats finaux
        print("\n" + "=" * 60)
        if success:
            logger.info("🎉 Migration terminée avec succès!")
            print("\n📋 Prochaines étapes:")
            print("1. Vérifier les données sur MongoDB Atlas et Neon")
            print("2. Adapter ton code pour utiliser les connexions cloud")
            print("3. Configurer les secrets sur Streamlit Community Cloud")
            print("4. Déployer ton application")
            print("\n🔗 Liens utiles:")
            print("- MongoDB Atlas: https://cloud.mongodb.com/")
            print("- Neon Console: https://console.neon.tech/")
            print("- Streamlit Cloud: https://share.streamlit.io/")
        else:
            logger.error("⚠️ Migration terminée avec des erreurs")
            logger.info("💡 Consulte les logs ci-dessus pour identifier les problèmes")
        
        return success
        
    except KeyboardInterrupt:
        logger.info("\n⏹️ Migration interrompue par l'utilisateur")
        return False
        
    except Exception as e:
        logger.error(f"💥 Erreur inattendue: {e}")
        return False
        
    finally:
        migrator.close_connections()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)