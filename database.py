import os
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.pool import QueuePool
from sqlalchemy.sql import func
from datetime import datetime

DATABASE_URL = os.getenv("DATABASE_URL")

# Ajuste para compatibilidade com Railway (postgres -> postgresql)
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if DATABASE_URL:
    engine = create_engine(
        DATABASE_URL,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800
    )
else:
    engine = create_engine("sqlite:///./sql_app.db")

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def init_db():
    Base.metadata.create_all(bind=engine)

# =========================================================
# ⚙️ CONFIGURAÇÕES GERAIS
# =========================================================
class SystemConfig(Base):
    __tablename__ = "system_config"
    key = Column(String, primary_key=True, index=True) 
    value = Column(String)                             
    updated_at = Column(DateTime, default=datetime.utcnow)

# =========================================================
# 🤖 BOTS
# =========================================================
class Bot(Base):
    __tablename__ = "bots"
    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String)
    token = Column(String, unique=True, index=True)
    username = Column(String, nullable=True)
    id_canal_vip = Column(String)
    admin_principal_id = Column(String, nullable=True)
    
    # 🔥 [NOVO] Username do Suporte
    suporte_username = Column(String, nullable=True)
    
    status = Column(String, default="ativo")
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # --- RELACIONAMENTOS (CASCADE) ---
    planos = relationship("PlanoConfig", back_populates="bot", cascade="all, delete-orphan")
    fluxo = relationship("BotFlow", back_populates="bot", uselist=False, cascade="all, delete-orphan")
    steps = relationship("BotFlowStep", back_populates="bot", cascade="all, delete-orphan")
    admins = relationship("BotAdmin", back_populates="bot", cascade="all, delete-orphan")
    
    # RELACIONAMENTOS PARA EXCLUSÃO AUTOMÁTICA
    pedidos = relationship("Pedido", backref="bot_ref", cascade="all, delete-orphan")
    leads = relationship("Lead", backref="bot_ref", cascade="all, delete-orphan")
    campanhas = relationship("RemarketingCampaign", backref="bot_ref", cascade="all, delete-orphan")
    
    # Relacionamento com Order Bump
    order_bump = relationship("OrderBumpConfig", uselist=False, back_populates="bot", cascade="all, delete-orphan")

class BotAdmin(Base):
    __tablename__ = "bot_admins"
    id = Column(Integer, primary_key=True, index=True)
    bot_id = Column(Integer, ForeignKey("bots.id"))
    telegram_id = Column(String)
    nome = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    bot = relationship("Bot", back_populates="admins")

# =========================================================
# 🛒 ORDER BUMP (OFERTA EXTRA NO CHECKOUT)
# =========================================================
class OrderBumpConfig(Base):
    __tablename__ = "order_bump_config"
    id = Column(Integer, primary_key=True, index=True)
    bot_id = Column(Integer, ForeignKey("bots.id"), unique=True)
    
    ativo = Column(Boolean, default=False)
    nome_produto = Column(String) # Nome do produto extra
    preco = Column(Float)         # Valor a ser somado
    link_acesso = Column(String, nullable=True) # Link do canal/grupo extra

    autodestruir = Column(Boolean, default=False)
    
    # Conteúdo da Oferta
    msg_texto = Column(Text, default="Gostaria de adicionar este item?")
    msg_media = Column(String, nullable=True)
    
    # Botões
    btn_aceitar = Column(String, default="✅ SIM, ADICIONAR")
    btn_recusar = Column(String, default="❌ NÃO, OBRIGADO")
    
    bot = relationship("Bot", back_populates="order_bump")

# =========================================================
# 💲 PLANOS
# =========================================================
class PlanoConfig(Base):
    __tablename__ = "planos_config"
    id = Column(Integer, primary_key=True, index=True)
    bot_id = Column(Integer, ForeignKey("bots.id"))
    
    key_id = Column(String, nullable=True) 
    nome_exibicao = Column(String)
    descricao = Column(String, nullable=True)
    preco_cheio = Column(Float, nullable=True)
    preco_atual = Column(Float)
    dias_duracao = Column(Integer)
    
    bot = relationship("Bot", back_populates="planos")

# =========================================================
# 📢 REMARKETING
# =========================================================
class RemarketingCampaign(Base):
    __tablename__ = "remarketing_campaigns"
    id = Column(Integer, primary_key=True, index=True)
    bot_id = Column(Integer, ForeignKey("bots.id"))
    campaign_id = Column(String, unique=True)
    target = Column(String, default="todos")
    type = Column(String, default="massivo")
    config = Column(String)
    status = Column(String, default="agendado")
    
    dia_atual = Column(Integer, default=0)
    data_inicio = Column(DateTime, default=datetime.utcnow)
    proxima_execucao = Column(DateTime, nullable=True)
    
    plano_id = Column(Integer, nullable=True)
    promo_price = Column(Float, nullable=True)
    expiration_at = Column(DateTime, nullable=True)
    
    total_leads = Column(Integer, default=0)
    sent_success = Column(Integer, default=0)
    blocked_count = Column(Integer, default=0)
    data_envio = Column(DateTime, default=datetime.utcnow)

# =========================================================
# 💬 FLUXO (ESTRUTURA HÍBRIDA V1 + V2)
# =========================================================
class BotFlow(Base):
    __tablename__ = "bot_flows"
    id = Column(Integer, primary_key=True, index=True)
    bot_id = Column(Integer, ForeignKey("bots.id"), unique=True)
    bot = relationship("Bot", back_populates="fluxo")
    
    # Passo 1 (Fixo)
    msg_boas_vindas = Column(Text, default="Olá! Bem-vindo.")
    media_url = Column(String, nullable=True)
    btn_text_1 = Column(String, default="🔓 DESBLOQUEAR")
    autodestruir_1 = Column(Boolean, default=False)
    
    # Mostrar Planos na Msg 1
    mostrar_planos_1 = Column(Boolean, default=False)
    
    # Passo Final (Fixo)
    msg_2_texto = Column(Text, nullable=True)
    msg_2_media = Column(String, nullable=True)
    mostrar_planos_2 = Column(Boolean, default=True)

# =========================================================
# 🧩 TABELA DE PASSOS INTERMEDIÁRIOS
# =========================================================
class BotFlowStep(Base):
    __tablename__ = "bot_flow_steps"
    id = Column(Integer, primary_key=True, index=True)
    bot_id = Column(Integer, ForeignKey("bots.id"))
    step_order = Column(Integer, default=1)
    msg_texto = Column(Text, nullable=True)
    msg_media = Column(String, nullable=True)
    btn_texto = Column(String, default="Próximo ▶️")
    
    # Controles de comportamento
    autodestruir = Column(Boolean, default=False)
    mostrar_botao = Column(Boolean, default=True)
    
    # Temporizador entre mensagens
    delay_seconds = Column(Integer, default=0)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    bot = relationship("Bot", back_populates="steps")

# =========================================================
# 🛒 PEDIDOS
# =========================================================
class Pedido(Base):
    __tablename__ = "pedidos"
    id = Column(Integer, primary_key=True, index=True)
    bot_id = Column(Integer, ForeignKey("bots.id"))
    
    telegram_id = Column(String)
    first_name = Column(String, nullable=True)
    username = Column(String, nullable=True)
    
    plano_nome = Column(String, nullable=True)
    plano_id = Column(Integer, nullable=True)
    valor = Column(Float)
    status = Column(String, default="pending") 
    
    txid = Column(String, unique=True, index=True) 
    qr_code = Column(Text, nullable=True)
    transaction_id = Column(String, nullable=True)
    
    data_aprovacao = Column(DateTime, nullable=True)
    data_expiracao = Column(DateTime, nullable=True)
    custom_expiration = Column(DateTime, nullable=True)
    
    link_acesso = Column(String, nullable=True)
    mensagem_enviada = Column(Boolean, default=False)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Campo para identificar se comprou o Order Bump
    tem_order_bump = Column(Boolean, default=False)
    
    # ============================================================
    # CAMPOS - FUNIL DE VENDAS
    # ============================================================
    status_funil = Column(String(20), default='meio')
    funil_stage = Column(String(20), default='lead_quente')
    
    primeiro_contato = Column(DateTime(timezone=True))
    escolheu_plano_em = Column(DateTime(timezone=True))
    gerou_pix_em = Column(DateTime(timezone=True))
    pagou_em = Column(DateTime(timezone=True))
    
    dias_ate_compra = Column(Integer, default=0)
    ultimo_remarketing = Column(DateTime(timezone=True))
    total_remarketings = Column(Integer, default=0)
    
    origem = Column(String(50), default='bot')


# =========================================================
# 🎯 TABELA: LEADS (TOPO DO FUNIL)
# =========================================================
class Lead(Base):
    """
    Tabela de Leads (TOPO do funil)
    Armazena usuários que APENAS deram /start no bot
    """
    __tablename__ = "leads"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, nullable=False)  # Telegram ID
    nome = Column(String)
    username = Column(String)
    bot_id = Column(Integer, ForeignKey('bots.id'))
    
    # Classificação
    status = Column(String(20), default='topo')
    funil_stage = Column(String(20), default='lead_frio')
    
    # Timestamps
    primeiro_contato = Column(DateTime(timezone=True), server_default=func.now())
    ultimo_contato = Column(DateTime(timezone=True))
    
    # Métricas
    total_remarketings = Column(Integer, default=0)
    ultimo_remarketing = Column(DateTime(timezone=True))
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())