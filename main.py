import os
import logging
import telebot
import requests
import time
import urllib.parse
import threading
from telebot import types
import json
import uuid

# --- IMPORTS CORRIGIDOS ---
from sqlalchemy import func, desc, text
from fastapi import FastAPI, HTTPException, Depends, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta
from database import Lead  # Não esqueça de importar Lead!


# Importa o banco e o script de reparo
from database import SessionLocal, init_db, Bot, PlanoConfig, BotFlow, BotFlowStep, Pedido, SystemConfig, RemarketingCampaign, BotAdmin, Lead, OrderBumpConfig, engine
import update_db 

from migration_v3 import executar_migracao_v3
from migration_v4 import executar_migracao_v4
from migration_v5 import executar_migracao_v5  # <--- ADICIONE ESTA LINHA
from migration_v6 import executar_migracao_v6  # <--- ADICIONE AQUI

# Configuração de Log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Zenyx Gbot SaaS")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# 1. FUNÇÃO DE CONEXÃO COM BANCO (TEM QUE SER A PRIMEIRA)
# =========================================================
def get_db():
    """Gera conexão com o banco de dados"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ============================================================
# 👇 COLE TODAS AS 5 FUNÇÕES AQUI (DEPOIS DO get_db)
# ============================================================

# FUNÇÃO 1: CRIAR OU ATUALIZAR LEAD (TOPO)
def criar_ou_atualizar_lead(
    db: Session,
    user_id: str,
    nome: str,
    username: str,
    bot_id: int
):
    """
    Cria ou atualiza um Lead quando usuário dá /start
    """
    lead = db.query(Lead).filter(
        Lead.user_id == user_id,
        Lead.bot_id == bot_id
    ).first()
    
    agora = datetime.utcnow()
    
    if lead:
        lead.ultimo_contato = agora
        lead.nome = nome
        lead.username = username
        logger.info(f"🔄 Lead atualizado: {nome} (ID: {user_id})")
    else:
        lead = Lead(
            user_id=user_id,
            nome=nome,
            username=username,
            bot_id=bot_id,
            primeiro_contato=agora,
            ultimo_contato=agora,
            status='topo',
            funil_stage='lead_frio'
        )
        db.add(lead)
        logger.info(f"✅ Novo LEAD criado: {nome} (TOPO - deu /start)")
    
    db.commit()
    db.refresh(lead)
    return lead


# FUNÇÃO 2: MOVER LEAD PARA PEDIDO (MEIO)
def mover_lead_para_pedido(
    db: Session,
    user_id: str,
    bot_id: int,
    pedido_id: int
):
    """
    Quando um Lead gera PIX, ele vira Pedido (MEIO)
    """
    lead = db.query(Lead).filter(
        Lead.user_id == user_id,
        Lead.bot_id == bot_id
    ).first()
    
    pedido = db.query(Pedido).filter(Pedido.id == pedido_id).first()
    
    if lead and pedido:
        pedido.primeiro_contato = lead.primeiro_contato
        pedido.escolheu_plano_em = datetime.utcnow()
        pedido.gerou_pix_em = datetime.utcnow()
        pedido.status_funil = 'meio'
        pedido.funil_stage = 'lead_quente'
        
        db.delete(lead)
        db.commit()
        logger.info(f"📊 Lead movido para MEIO (Pedido): {pedido.first_name}")
    
    return pedido


# FUNÇÃO 3: MARCAR COMO PAGO (FUNDO)
def marcar_como_pago(
    db: Session,
    pedido_id: int
):
    """
    Marca pedido como PAGO (FUNDO do funil)
    """
    pedido = db.query(Pedido).filter(Pedido.id == pedido_id).first()
    
    if not pedido:
        return None
    
    agora = datetime.utcnow()
    pedido.pagou_em = agora
    pedido.status_funil = 'fundo'
    pedido.funil_stage = 'cliente'
    
    if pedido.primeiro_contato:
        dias = (agora - pedido.primeiro_contato).days
        pedido.dias_ate_compra = dias
        logger.info(f"✅ PAGAMENTO APROVADO! {pedido.first_name} - Dias até compra: {dias}")
    else:
        pedido.dias_ate_compra = 0
    
    db.commit()
    db.refresh(pedido)
    return pedido


# FUNÇÃO 4: MARCAR COMO EXPIRADO
def marcar_como_expirado(
    db: Session,
    pedido_id: int
):
    """
    Marca pedido como EXPIRADO (PIX venceu)
    """
    pedido = db.query(Pedido).filter(Pedido.id == pedido_id).first()
    
    if pedido:
        pedido.status_funil = 'expirado'
        pedido.funil_stage = 'lead_quente'
        db.commit()
        logger.info(f"⏰ PIX EXPIRADO: {pedido.first_name}")
    
    return pedido


# FUNÇÃO 5: REGISTRAR REMARKETING
def registrar_remarketing(
    db: Session,
    user_id: str,
    bot_id: int
):
    """
    Registra que usuário recebeu remarketing
    """
    agora = datetime.utcnow()
    
    # Atualiza Lead (se for TOPO)
    lead = db.query(Lead).filter(
        Lead.user_id == user_id,
        Lead.bot_id == bot_id
    ).first()
    
    if lead:
        lead.ultimo_remarketing = agora
        lead.total_remarketings += 1
        db.commit()
        logger.info(f"📧 Remarketing registrado (TOPO): {lead.nome}")
        return
    
    # Atualiza Pedido (se for MEIO/EXPIRADO)
    pedido = db.query(Pedido).filter(
        Pedido.telegram_id == user_id,
        Pedido.bot_id == bot_id
    ).first()
    
    if pedido:
        pedido.ultimo_remarketing = agora
        pedido.total_remarketings += 1
        db.commit()
        logger.info(f"📧 Remarketing registrado (MEIO): {pedido.first_name}")


# =========================================================
# 2. AUTO-REPARO DO BANCO DE DADOS (LISTA MESTRA DE CORREÇÃO)
# =========================================================
@app.on_event("startup")
def on_startup():
    init_db()
    executar_migracao_v3()
    executar_migracao_v4()
    executar_migracao_v5()  # <--- ADICIONE ESTA LINHA
    executar_migracao_v6() # <--- ADICIONE AQUI

    # 2. FORÇA A CRIAÇÃO DE TODAS AS COLUNAS FALTANTES (TODAS AS VERSÕES)
    try:
        with engine.connect() as conn:
            logger.info("🔧 [STARTUP] Verificando integridade completa do banco...")
            
            comandos_sql = [
                # --- [CORREÇÃO 1] TABELA DE PLANOS (Causa do erro ao Criar Plano) ---
                "ALTER TABLE planos_config ADD COLUMN IF NOT EXISTS key_id VARCHAR;",
                "ALTER TABLE planos_config ADD COLUMN IF NOT EXISTS descricao TEXT;",
                "ALTER TABLE planos_config ADD COLUMN IF NOT EXISTS preco_cheio FLOAT;",

                # --- [CORREÇÃO 2] TABELA DE PEDIDOS (Datas e Frontend) ---
                "ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS plano_id INTEGER;",
                "ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS plano_nome VARCHAR;",
                "ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS txid VARCHAR;",
                "ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS qr_code TEXT;",
                "ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS transaction_id VARCHAR;", 
                "ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS data_aprovacao TIMESTAMP WITHOUT TIME ZONE;",
                "ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS data_expiracao TIMESTAMP WITHOUT TIME ZONE;",
                "ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS custom_expiration TIMESTAMP WITHOUT TIME ZONE;", # <--- CRÍTICO PRO FRONTEND
                "ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS link_acesso VARCHAR;",
                "ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS mensagem_enviada BOOLEAN DEFAULT FALSE;",

                # --- [CORREÇÃO 3] FLUXO DE MENSAGENS (Antigo mas necessário) ---
                "ALTER TABLE bot_flows ADD COLUMN IF NOT EXISTS autodestruir_1 BOOLEAN DEFAULT FALSE;",
                "ALTER TABLE bot_flows ADD COLUMN IF NOT EXISTS msg_2_texto TEXT;",
                "ALTER TABLE bot_flows ADD COLUMN IF NOT EXISTS msg_2_media VARCHAR;",
                "ALTER TABLE bot_flows ADD COLUMN IF NOT EXISTS mostrar_planos_2 BOOLEAN DEFAULT TRUE;",
                
                # --- [CORREÇÃO NOVA] MOSTRAR PLANOS NO PASSO 1 ---
                "ALTER TABLE bot_flows ADD COLUMN IF NOT EXISTS mostrar_planos_1 BOOLEAN DEFAULT FALSE;",
                
                # --- [CORREÇÃO 4] REMARKETING AVANÇADO (CRÍTICO) ---
                "ALTER TABLE remarketing_campaigns ADD COLUMN IF NOT EXISTS target VARCHAR DEFAULT 'todos';",
                "ALTER TABLE remarketing_campaigns ADD COLUMN IF NOT EXISTS type VARCHAR DEFAULT 'massivo';",
                "ALTER TABLE remarketing_campaigns ADD COLUMN IF NOT EXISTS plano_id INTEGER;",
                "ALTER TABLE remarketing_campaigns ADD COLUMN IF NOT EXISTS promo_price FLOAT;",
                "ALTER TABLE remarketing_campaigns ADD COLUMN IF NOT EXISTS expiration_at TIMESTAMP WITHOUT TIME ZONE;",
                "ALTER TABLE remarketing_campaigns ADD COLUMN IF NOT EXISTS dia_atual INTEGER DEFAULT 0;",
                "ALTER TABLE remarketing_campaigns ADD COLUMN IF NOT EXISTS data_inicio TIMESTAMP WITHOUT TIME ZONE DEFAULT now();",
                "ALTER TABLE remarketing_campaigns ADD COLUMN IF NOT EXISTS proxima_execucao TIMESTAMP WITHOUT TIME ZONE;",
                
                # --- [CORREÇÃO 5] TABELA NOVA (FLOW V2) ---
                """
                CREATE TABLE IF NOT EXISTS bot_flow_steps (
                    id SERIAL PRIMARY KEY,
                    bot_id INTEGER REFERENCES bots(id),
                    step_order INTEGER DEFAULT 1,
                    msg_texto TEXT,
                    msg_media VARCHAR,
                    btn_texto VARCHAR DEFAULT 'Próximo ▶️',
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
                );
                """
            ]
            
            for cmd in comandos_sql:
                try:
                    conn.execute(text(cmd))
                    conn.commit()
                except Exception as e_sql:
                    # Ignora erro se a coluna já existir (segurança)
                    logger.warning(f"Aviso SQL: {e_sql}")
            
            logger.info("✅ [STARTUP] Banco de dados 100% Verificado!")
            
    except Exception as e:
        logger.error(f"❌ Falha no reparo do banco: {e}")

    # 3. Inicia o Ceifador
    thread = threading.Thread(target=loop_verificar_vencimentos)
    thread.daemon = True
    thread.start()
    logger.info("💀 O Ceifador (Auto-Kick) foi iniciado!")

# =========================================================
# 💀 O CEIFADOR: VERIFICA VENCIMENTOS E REMOVE (KICK SUAVE)
# =========================================================
def loop_verificar_vencimentos():
    """Roda a cada 60 minutos para remover usuários vencidos"""
    while True:
        try:
            logger.info("⏳ Verificando assinaturas vencidas...")
            verificar_expiracao_massa()
        except Exception as e:
            logger.error(f"Erro no loop de vencimento: {e}")
        
        time.sleep(3600) # Espera 1 hora (3600 segundos)

# =========================================================
# 💀 O CEIFADOR: REMOVEDOR BASEADO EM DATA (SAAS)
# =========================================================
def verificar_expiracao_massa():
    db = SessionLocal()
    try:
        # Pega todos os bots do sistema
        bots = db.query(Bot).all()
        
        for bot_data in bots:
            if not bot_data.token or not bot_data.id_canal_vip: continue
            
            try:
                # Conecta no Telegram deste bot específico
                tb = telebot.TeleBot(bot_data.token)
                
                # Tratamento do ID do canal
                try: canal_id = int(str(bot_data.id_canal_vip).strip())
                except: canal_id = bot_data.id_canal_vip
                
                agora = datetime.utcnow()
                
                # --- QUERY INTELIGENTE ---
                # Busca usuários deste bot que:
                # 1. Estão com status 'paid' (Ativos)
                # 2. Têm uma data de expiração definida (NÃO SÃO VITALÍCIOS)
                # 3. A data de expiração é MENOR que agora (Já venceu)
                vencidos = db.query(Pedido).filter(
                    Pedido.bot_id == bot_data.id,
                    Pedido.status == 'paid',
                    Pedido.custom_expiration != None, 
                    Pedido.custom_expiration < agora
                ).all()
                
                for u in vencidos:
                    # 🔥 [CORRIGIDO] Proteção: Se for admin, não remove
                    # Verificar se é admin através da tabela BotAdmin
                    admin = db.query(BotAdmin).filter(
                        BotAdmin.telegram_id == u.telegram_id,
                        BotAdmin.bot_id == u.bot_id
                    ).first()
                    
                    if admin:
                        logger.info(f"⏭️ Pulando admin: {u.telegram_id}")
                        continue
                    
                    try:
                        logger.info(f"💀 Ceifando usuário vencido: {u.first_name} (Bot: {bot_data.nome})")
                        
                        # 1. Kick Suave (Ban + Unban)
                        tb.ban_chat_member(canal_id, int(u.telegram_id))
                        tb.unban_chat_member(canal_id, int(u.telegram_id))
                        
                        # 2. Atualiza Status no Banco
                        u.status = 'expired'
                        db.commit()
                        
                        # 3. Avisa o defunto
                        try: 
                            tb.send_message(int(u.telegram_id), "🚫 <b>Seu plano venceu!</b>\n\nSeu tempo acabou. Para renovar, digite /start", parse_mode="HTML")
                        except: pass
                        
                    except Exception as e_kick:
                        logger.error(f"Erro ao remover {u.telegram_id}: {e_kick}")
                        # Se der erro (ex: user já saiu), marca como expired para não ficar tentando eternamente
                        u.status = 'expired'
                        db.commit()
                        
            except Exception as e_bot:
                logger.error(f"Erro ao processar bot {bot_data.id}: {e_bot}")
                
    finally: 
        db.close()

# =========================================================
# 🔌 INTEGRAÇÃO PUSHIN PAY (DINÂMICA)
# =========================================================
def get_pushin_token():
    """Busca o token no banco, se não achar, tenta variável de ambiente"""
    db = SessionLocal()
    try:
        # Tenta pegar do banco de dados (Painel de Integrações)
        config = db.query(SystemConfig).filter(SystemConfig.key == "pushin_pay_token").first()
        if config and config.value:
            return config.value
        # Se não tiver no banco, pega do Railway Variables
        return os.getenv("PUSHIN_PAY_TOKEN")
    finally:
        db.close()

# =========================================================
# 🔌 INTEGRAÇÃO PUSHIN PAY (CORRIGIDA)
# =========================================================
def gerar_pix_pushinpay(valor_float: float, transaction_id: str):
    token = get_pushin_token()
    
    if not token:
        logger.error("❌ Token Pushin Pay não configurado!")
        return None
    
    url = "https://api.pushinpay.com.br/api/pix/cashIn"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    # URL DO RAILWAY FIXA (Garante que o Webhook chegue)
    seus_dominio = "zenyx-gbs-testes-production.up.railway.app" 
    
    payload = {
        "value": int(valor_float * 100), 
        "webhook_url": f"https://{seus_dominio}/webhook/pix",
        "external_reference": transaction_id
    }

    try:
        logger.info(f"📤 Gerando PIX. Webhook definido para: https://{seus_dominio}/webhook/pix")
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        
        if response.status_code in [200, 201]:
            return response.json()
        else:
            logger.error(f"Erro PushinPay: {response.text}")
            return None
    except Exception as e:
        logger.error(f"Exceção PushinPay: {e}")
        return None

# --- HELPER: Notificar Admin Principal ---
# --- HELPER: Notificar TODOS os Admins (Principal + Extras) ---
# --- HELPER: Notificar TODOS os Admins (Principal + Extras) ---
def notificar_admin_principal(bot_db: Bot, mensagem: str):
    """
    Envia notificação para o Admin Principal E para os Admins Extras configurados.
    """
    ids_unicos = set()

    # 1. Adiciona Admin Principal (se houver)
    if bot_db.admin_principal_id:
        ids_unicos.add(str(bot_db.admin_principal_id).strip())

    # 2. Adiciona Admins Extras (da tabela BotAdmin)
    if bot_db.admins:
        for admin in bot_db.admins:
            if admin.telegram_id:
                ids_unicos.add(str(admin.telegram_id).strip())

    if not ids_unicos:
        return

    try:
        sender = telebot.TeleBot(bot_db.token)
        for chat_id in ids_unicos:
            try:
                # 🔥 ALTERADO PARA HTML
                sender.send_message(chat_id, mensagem, parse_mode="HTML")
            except Exception as e_send:
                logger.error(f"Erro ao notificar admin {chat_id}: {e_send}")
                
    except Exception as e:
        logger.error(f"Falha geral na notificação: {e}")

# --- ROTAS DE INTEGRAÇÃO (SALVAR TOKEN) ---
# =========================================================
# 🔌 ROTAS DE INTEGRAÇÃO (SALVAR TOKEN PUSHIN PAY)
# =========================================================

# Modelo para receber o JSON do frontend
class IntegrationUpdate(BaseModel):
    token: str

@app.get("/api/admin/integrations/pushinpay")
def get_pushin_status(db: Session = Depends(get_db)):
    # Busca token no banco
    config = db.query(SystemConfig).filter(SystemConfig.key == "pushin_pay_token").first()
    
    # Se não achar no banco, tenta variável de ambiente (backup)
    token = config.value if config else os.getenv("PUSHIN_PAY_TOKEN")
    
    if not token:
        return {"status": "desconectado", "token_mask": ""}
    
    # Cria máscara para segurança (ex: "abc1...890")
    mask = f"{token[:4]}...{token[-4:]}" if len(token) > 8 else "****"
    return {"status": "conectado", "token_mask": mask}

@app.post("/api/admin/integrations/pushinpay")
def save_pushin_token(data: IntegrationUpdate, db: Session = Depends(get_db)):
    # 1. Busca ou Cria a configuração
    config = db.query(SystemConfig).filter(SystemConfig.key == "pushin_pay_token").first()
    if not config:
        config = SystemConfig(key="pushin_pay_token")
        db.add(config)
    
    # 2. Limpa espaços em branco acidentais
    token_limpo = data.token.strip()
    
    # 3. Validação básica
    if len(token_limpo) < 10:
        return {"status": "erro", "msg": "Token muito curto ou inválido."}

    # 4. Salva
    config.value = token_limpo
    config.updated_at = datetime.utcnow()
    db.commit()
    
    logger.info(f"🔑 Token PushinPay atualizado: {token_limpo[:5]}...")
    
    return {"status": "conectado", "msg": "Integração salva com sucesso!"}

# --- MODELOS ---
class BotCreate(BaseModel):
    nome: str
    token: str
    id_canal_vip: str
    admin_principal_id: Optional[str] = None  # Campo opcional

# Novo modelo para Atualização
class BotUpdate(BaseModel):
    nome: Optional[str] = None
    token: Optional[str] = None
    id_canal_vip: Optional[str] = None
    admin_principal_id: Optional[str] = None

# Modelo para Criar Admin
class BotAdminCreate(BaseModel):
    telegram_id: str
    nome: Optional[str] = "Admin"

class BotResponse(BotCreate):
    id: int
    status: str
    leads: int = 0
    revenue: float = 0.0
    class Config:
        from_attributes = True

class PlanoCreate(BaseModel):
    bot_id: int
    nome_exibicao: str
    preco: float
    dias_duracao: int

# --- Adicione logo após a classe PlanoCreate ---
# 👇 COLE ISSO LOGO ABAIXO DA CLASS 'PlanoCreate'
# COLE AQUI (LOGO APÓS AS CLASSES INICIAIS)
class PlanoUpdate(BaseModel):
    nome_exibicao: Optional[str] = None
    preco: Optional[float] = None
    dias_duracao: Optional[int] = None
    
    # Adiciona essa config para permitir que o Pydantic ignore tipos estranhos se possível
    class Config:
        arbitrary_types_allowed = True
class FlowUpdate(BaseModel):
    msg_boas_vindas: str
    media_url: Optional[str] = None
    btn_text_1: str
    autodestruir_1: bool
    msg_2_texto: Optional[str] = None
    msg_2_media: Optional[str] = None
    mostrar_planos_2: bool
    mostrar_planos_1: Optional[bool] = False # 🔥 NOVO CAMPO

class FlowStepCreate(BaseModel):
    msg_texto: str
    msg_media: Optional[str] = None
    btn_texto: str = "Próximo ▶️"
    step_order: int

class FlowStepUpdate(BaseModel):
    """Modelo para atualizar um passo existente"""
    msg_texto: Optional[str] = None
    msg_media: Optional[str] = None
    btn_texto: Optional[str] = None
    autodestruir: Optional[bool] = None      # [NOVO V3]
    mostrar_botao: Optional[bool] = None     # [NOVO V3]
    delay_seconds: Optional[int] = None  # [NOVO V4]


class UserUpdateCRM(BaseModel):
    first_name: Optional[str] = None
    username: Optional[str] = None
    # Recebe a data como string do frontend
    custom_expiration: Optional[str] = None 
    status: Optional[str] = None

# --- MODELOS ORDER BUMP ---
class OrderBumpCreate(BaseModel):
    ativo: bool
    nome_produto: str
    preco: float
    link_acesso: str
    autodestruir: Optional[bool] = False  # <--- ADICIONE AQUI
    msg_texto: Optional[str] = None
    msg_media: Optional[str] = None
    btn_aceitar: Optional[str] = "✅ SIM, ADICIONAR"
    btn_recusar: Optional[str] = "❌ NÃO, OBRIGADO"

class IntegrationUpdate(BaseModel):
    token: str

# --- MODELO DE PERFIL ---
class ProfileUpdate(BaseModel):
    name: str
    avatar_url: Optional[str] = None

# ✅ MODELO COMPLETO PARA O WIZARD DE REMARKETING
# =========================================================
# ✅ MODELO DE DADOS (ESPELHO DO REMARKETING.JSX)
# =========================================================
class RemarketingRequest(BaseModel):
    bot_id: int
    # O Frontend manda 'target', contendo: 'todos', 'pendentes', 'pagantes' ou 'expirados'
    target: str = "todos" 
    mensagem: str
    media_url: Optional[str] = None
    
    # Oferta (Alinhado com o JSX)
    incluir_oferta: bool = False
    plano_oferta_id: Optional[str] = None
    
    # Preço e Validade (Alinhado com o JSX)
    price_mode: str = "original" # 'original' ou 'custom'
    custom_price: Optional[float] = 0.0
    expiration_mode: str = "none" # 'none', 'minutes', 'hours', 'days'
    expiration_value: Optional[int] = 0
    
    # Controle (Isso vem do api.js na função sendRemarketing)
    is_test: bool = False
    specific_user_id: Optional[str] = None

    # Campos de compatibilidade (Opcionais, pois seu frontend NÃO está mandando isso agora)
    tipo_envio: Optional[str] = None 
    expire_timestamp: Optional[int] = 0

    # ---   
# Modelo para Atualização de Usuário (CRM)
class UserUpdate(BaseModel):
    role: Optional[str] = None
    status: Optional[str] = None
    custom_expiration: Optional[str] = None # 'vitalicio', 'remover' ou data YYYY-MM-DD

# ===========================
# ⚙️ GESTÃO DE BOTS
# ===========================

@app.post("/api/admin/bots", response_model=BotResponse)
def criar_bot(bot_data: BotCreate, db: Session = Depends(get_db)):
    if db.query(Bot).filter(Bot.token == bot_data.token).first():
        raise HTTPException(status_code=400, detail="Token já cadastrado.")

    try:
        tb = telebot.TeleBot(bot_data.token)
        
        # [NOVO] Busca informações do bot do Telegram
        bot_info = tb.get_me()
        username = bot_info.username if hasattr(bot_info, 'username') else None
        
        # Configura webhook
        public_url = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
        if public_url:
            webhook_url = f"https://{public_url}/webhook/{bot_data.token}"
            tb.set_webhook(url=webhook_url)
        
        status = "ativo"
    except Exception as e:
        logger.error(f"Erro ao criar bot: {e}")
        raise HTTPException(status_code=400, detail="Token inválido ou erro ao conectar.")

    novo_bot = Bot(
        nome=bot_data.nome,
        token=bot_data.token,
        username=username,  # [CORRIGIDO] Salva o username do Telegram
        id_canal_vip=bot_data.id_canal_vip,
        status=status,
        admin_principal_id=bot_data.admin_principal_id
    )
    db.add(novo_bot)
    db.commit()
    db.refresh(novo_bot)
    
    # [NOVO] Retorna com username incluído
    return {
        "id": novo_bot.id,
        "nome": novo_bot.nome,
        "token": novo_bot.token,
        "username": novo_bot.username,
        "id_canal_vip": novo_bot.id_canal_vip,
        "admin_principal_id": novo_bot.admin_principal_id,
        "status": novo_bot.status,
        "leads": 0,
        "revenue": 0.0,
        "created_at": novo_bot.created_at
    }

@app.put("/api/admin/bots/{bot_id}")
def update_bot(bot_id: int, dados: BotCreate, db: Session = Depends(get_db)):
    bot_db = db.query(Bot).filter(Bot.id == bot_id).first()
    if not bot_db: raise HTTPException(404, "Bot não encontrado")
    
    # Guarda token antigo para verificar mudança
    old_token = bot_db.token

    # 1. Atualiza campos administrativos básicos (Canal e Admin)
    if dados.id_canal_vip: 
        bot_db.id_canal_vip = dados.id_canal_vip
    
    if dados.admin_principal_id is not None: 
        bot_db.admin_principal_id = dados.admin_principal_id
    
    # 2. LÓGICA DE TROCA DE TOKEN (COM AUTO-ATUALIZAÇÃO DE NOME E USERNAME)
    if dados.token and dados.token != old_token:
        try:
            logger.info(f"🔄 Detectada troca de token para o bot ID {bot_id}...")
            
            # A) Conecta com o NOVO token para validar e pegar dados reais
            new_tb = telebot.TeleBot(dados.token)
            bot_info = new_tb.get_me() # <--- A MÁGICA ACONTECE AQUI
            
            # B) Atualiza Token, Nome e Username baseados no Telegram
            bot_db.token = dados.token
            bot_db.nome = bot_info.first_name # Atualiza o nome automaticamente
            bot_db.username = bot_info.username # Atualiza o @ automaticamente
            
            logger.info(f"✅ Dados atualizados via Telegram: {bot_db.nome} (@{bot_db.username})")

            # C) Remove o Webhook do token ANTIGO (Limpeza)
            try:
                old_tb = telebot.TeleBot(old_token)
                old_tb.delete_webhook()
            except: 
                pass

            # D) Configura o Webhook no NOVO token
            public_url = os.getenv("RAILWAY_PUBLIC_DOMAIN", "https://zenyx-gbs-testes-production.up.railway.app")
            # Remove https:// se vier duplicado da env var
            if public_url.startswith("https://"): public_url = public_url.replace("https://", "")
            
            webhook_url = f"https://{public_url}/webhook/{dados.token}"
            new_tb.set_webhook(url=webhook_url)
            
            logger.info(f"♻️ Webhook atualizado com sucesso para: {webhook_url}")
            bot_db.status = "ativo" # Reseta para ativo ao trocar token
            
        except Exception as e:
            logger.error(f"❌ Erro ao validar novo token: {e}")
            # Se o token for inválido, não salvamos a troca para não quebrar o bot
            raise HTTPException(status_code=400, detail=f"Token inválido ou erro de conexão com Telegram: {str(e)}")
            
    else:
        # Se NÃO trocou o token, permite atualizar o nome manualmente pelo input
        if dados.nome: 
            bot_db.nome = dados.nome
    
    db.commit()
    db.refresh(bot_db)
    return {"status": "ok", "msg": "Bot atualizado com sucesso"}

# --- NOVA ROTA: LIGAR/DESLIGAR BOT (TOGGLE) ---
# --- NOVA ROTA: LIGAR/DESLIGAR BOT (TOGGLE) ---
@app.post("/api/admin/bots/{bot_id}/toggle")
def toggle_bot(bot_id: int, db: Session = Depends(get_db)):
    bot = db.query(Bot).filter(Bot.id == bot_id).first()
    if not bot: raise HTTPException(404, "Bot não encontrado")
    
    # Inverte o status
    novo_status = "ativo" if bot.status != "ativo" else "pausado"
    bot.status = novo_status
    db.commit()
    
    # 🔔 Notifica Admin (EM HTML)
    try:
        emoji = "🟢" if novo_status == "ativo" else "🔴"
        # 🔥 Texto formatado com tags HTML
        msg = f"{emoji} <b>STATUS DO BOT ALTERADO</b>\n\nO bot <b>{bot.nome}</b> agora está: <b>{novo_status.upper()}</b>"
        notificar_admin_principal(bot, msg)
    except Exception as e:
        logger.error(f"Erro ao notificar admin sobre toggle: {e}")
    
    return {"status": novo_status}

# --- NOVA ROTA: EXCLUIR BOT ---
# --- NOVA ROTA: EXCLUIR BOT (COM LIMPEZA TOTAL) ---
@app.delete("/api/admin/bots/{bot_id}")
def deletar_bot(bot_id: int, db: Session = Depends(get_db)):
    bot_db = db.query(Bot).filter(Bot.id == bot_id).first()
    if not bot_db:
        raise HTTPException(status_code=404, detail="Bot não encontrado")
    
    try:
        # 1. Tenta remover o Webhook do Telegram para limpar no servidor deles
        try:
            tb = telebot.TeleBot(bot_db.token)
            tb.delete_webhook()
        except:
            pass # Se der erro (ex: token inválido), continua e apaga do banco
        
        # 2. LIMPEZA PESADA (Exclui manualmente os dependentes para evitar erro de integridade)
        # Apaga PEDIDOS vinculados
        db.query(Pedido).filter(Pedido.bot_id == bot_id).delete(synchronize_session=False)
        
        # Apaga LEADS vinculados
        db.query(Lead).filter(Lead.bot_id == bot_id).delete(synchronize_session=False)
        
        # Apaga CAMPANHAS de Remarketing vinculadas
        db.query(RemarketingCampaign).filter(RemarketingCampaign.bot_id == bot_id).delete(synchronize_session=False)
        
        # 3. Apaga o Bot (Planos, Fluxos e Admins já caem por cascade)
        db.delete(bot_db)
        db.commit()
        
        logger.info(f"🗑️ Bot {bot_id} e todos os seus dados foram excluídos com sucesso.")
        return {"status": "deleted", "msg": "Bot e todos os dados vinculados removidos com sucesso"}
        
    except Exception as e:
        db.rollback() # Desfaz se der erro no meio do caminho
        logger.error(f"Erro ao deletar bot {bot_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno ao excluir bot: {str(e)}")

# =========================================================
# 🛡️ GESTÃO DE ADMINISTRADORES (FASE 2 - COM EDIÇÃO)
# =========================================================

@app.get("/api/admin/bots/{bot_id}/admins")
def listar_admins(bot_id: int, db: Session = Depends(get_db)):
    """Lista todos os admins de um bot específico"""
    admins = db.query(BotAdmin).filter(BotAdmin.bot_id == bot_id).all()
    return admins

@app.post("/api/admin/bots/{bot_id}/admins")
def adicionar_admin(bot_id: int, dados: BotAdminCreate, db: Session = Depends(get_db)):
    """Adiciona um novo admin ao bot"""
    # Verifica se o bot existe
    bot = db.query(Bot).filter(Bot.id == bot_id).first()
    if not bot:
        raise HTTPException(status_code=404, detail="Bot não encontrado")
    
    # Verifica se já é admin
    existente = db.query(BotAdmin).filter(
        BotAdmin.bot_id == bot_id, 
        BotAdmin.telegram_id == dados.telegram_id
    ).first()
    
    if existente:
        raise HTTPException(status_code=400, detail="Este ID já é administrador deste bot.")
    
    novo_admin = BotAdmin(
        bot_id=bot_id,
        telegram_id=dados.telegram_id,
        nome=dados.nome
    )
    db.add(novo_admin)
    db.commit()
    db.refresh(novo_admin)
    return novo_admin

# 🔥 [NOVO] Rota para Editar Admin Existente
@app.put("/api/admin/bots/{bot_id}/admins/{admin_id}")
def atualizar_admin(bot_id: int, admin_id: int, dados: BotAdminCreate, db: Session = Depends(get_db)):
    """Atualiza o ID ou Nome de um administrador existente"""
    admin_db = db.query(BotAdmin).filter(
        BotAdmin.id == admin_id,
        BotAdmin.bot_id == bot_id
    ).first()
    
    if not admin_db:
        raise HTTPException(status_code=404, detail="Administrador não encontrado")
    
    # Verifica duplicidade (se mudar o ID para um que já existe)
    if dados.telegram_id != admin_db.telegram_id:
        existente = db.query(BotAdmin).filter(
            BotAdmin.bot_id == bot_id, 
            BotAdmin.telegram_id == dados.telegram_id
        ).first()
        if existente:
            raise HTTPException(status_code=400, detail="Este Telegram ID já está em uso.")

    # Atualiza os dados
    admin_db.telegram_id = dados.telegram_id
    admin_db.nome = dados.nome
    
    db.commit()
    db.refresh(admin_db)
    return admin_db

@app.delete("/api/admin/bots/{bot_id}/admins/{telegram_id}")
def remover_admin(bot_id: int, telegram_id: str, db: Session = Depends(get_db)):
    """Remove um admin pelo Telegram ID"""
    admin_db = db.query(BotAdmin).filter(
        BotAdmin.bot_id == bot_id,
        BotAdmin.telegram_id == telegram_id
    ).first()
    
    if not admin_db:
        raise HTTPException(status_code=404, detail="Administrador não encontrado")
    
    db.delete(admin_db)
    db.commit()
    return {"status": "deleted", "msg": "Administrador removido com sucesso"}

# --- NOVA ROTA: LISTAR BOTS ---

# =========================================================
# 🤖 LISTAR BOTS (COM KPI TOTAIS E USERNAME CORRIGIDO)
# =========================================================
# ============================================================
# 🔥 ROTA CORRIGIDA: /api/admin/bots
# SUBSTITUA a rota existente no main.py
# CORRIGE: Conta LEADS + PEDIDOS (sem duplicatas)
# ============================================================

@app.get("/api/admin/bots")
def listar_bots(db: Session = Depends(get_db)):
    """
    🔥 [CORRIGIDO] Lista todos os bots com estatísticas corretas
    
    CORREÇÕES:
    - Leads: Conta LEADS (tabela Lead) + PEDIDOS (tabela Pedido) SEM DUPLICATAS
    - Revenue: Soma apenas pedidos com status 'approved', 'paid', 'active'
    - Username: Garante que sempre retorna o username
    """
    from sqlalchemy import func
    
    bots = db.query(Bot).all()
    
    result = []
    for bot in bots:
        # ============================================================
        # 🔥 [CORRIGIDO] Conta LEADS + PEDIDOS (sem duplicatas)
        # Usa a mesma lógica da página Contatos (filtro TODOS)
        # ============================================================
        
        # 1. Buscar todos os telegram_ids de LEADS
        leads_ids = set()
        leads_query = db.query(Lead.user_id).filter(Lead.bot_id == bot.id).all()
        for lead in leads_query:
            leads_ids.add(str(lead.user_id))
        
        # 2. Buscar todos os telegram_ids de PEDIDOS
        pedidos_ids = set()
        pedidos_query = db.query(Pedido.telegram_id).filter(Pedido.bot_id == bot.id).all()
        for pedido in pedidos_query:
            pedidos_ids.add(str(pedido.telegram_id))
        
        # 3. União dos sets (elimina duplicatas automaticamente)
        # Se um lead virou pedido, conta apenas 1 vez
        contatos_unicos = leads_ids.union(pedidos_ids)
        leads_count = len(contatos_unicos)
        
        logger.info(f"📊 Bot {bot.nome}: {len(leads_ids)} leads + {len(pedidos_ids)} pedidos = {leads_count} contatos únicos")
        
        # ============================================================
        # 💰 [MANTIDO] Revenue: Soma vendas aprovadas
        # ============================================================
        vendas_aprovadas = db.query(Pedido).filter(
            Pedido.bot_id == bot.id,
            Pedido.status.in_(["approved", "paid", "active"])
        ).all()
        revenue = sum([v.valor for v in vendas_aprovadas]) if vendas_aprovadas else 0.0
        
        result.append({
            "id": bot.id,
            "nome": bot.nome,
            "token": bot.token,
            "username": bot.username or None,
            "id_canal_vip": bot.id_canal_vip,
            "admin_principal_id": bot.admin_principal_id,
            "status": bot.status,
            "leads": leads_count,        # 🔥 [CORRIGIDO] Leads + Pedidos únicos
            "revenue": revenue,
            "created_at": bot.created_at
        })
    
    return result


# ===========================
# 💎 PLANOS & FLUXO
# ===========================

@app.post("/api/admin/plans")
def criar_plano(plano: PlanoCreate, db: Session = Depends(get_db)):
    novo_plano = PlanoConfig(
        bot_id=plano.bot_id,
        key_id=f"plan_{plano.bot_id}_{plano.dias_duracao}d",
        nome_exibicao=plano.nome_exibicao,
        descricao=f"Acesso de {plano.dias_duracao} dias",
        preco_cheio=plano.preco * 2,
        preco_atual=plano.preco,
        dias_duracao=plano.dias_duracao
    )
    db.add(novo_plano)
    db.commit()
    return {"status": "ok"}

@app.get("/api/admin/plans/{bot_id}")
def listar_planos(bot_id: int, db: Session = Depends(get_db)):
    return db.query(PlanoConfig).filter(PlanoConfig.bot_id == bot_id).all()

# =========================================================
# 🛒 ORDER BUMP API
# =========================================================
@app.get("/api/admin/bots/{bot_id}/order-bump")
def get_order_bump(bot_id: int, db: Session = Depends(get_db)):
    bump = db.query(OrderBumpConfig).filter(OrderBumpConfig.bot_id == bot_id).first()
    if not bump:
        # Retorna objeto vazio padrão se não existir
        return {
            "ativo": False, "nome_produto": "", "preco": 0.0, "link_acesso": "",
            "msg_texto": "", "msg_media": "", 
            "btn_aceitar": "✅ SIM, ADICIONAR", "btn_recusar": "❌ NÃO, OBRIGADO"
        }
    return bump

@app.post("/api/admin/bots/{bot_id}/order-bump")
def save_order_bump(bot_id: int, dados: OrderBumpCreate, db: Session = Depends(get_db)):
    bump = db.query(OrderBumpConfig).filter(OrderBumpConfig.bot_id == bot_id).first()
    
    if not bump:
        bump = OrderBumpConfig(bot_id=bot_id)
        db.add(bump)
    
    bump.ativo = dados.ativo
    bump.nome_produto = dados.nome_produto
    bump.preco = dados.preco
    bump.link_acesso = dados.link_acesso
    bump.autodestruir = dados.autodestruir  # <--- ADICIONE AQUI
    bump.msg_texto = dados.msg_texto
    bump.msg_media = dados.msg_media
    bump.btn_aceitar = dados.btn_aceitar
    bump.btn_recusar = dados.btn_recusar
    
    db.commit()
    db.refresh(bump)
    return {"status": "ok", "msg": "Order Bump salvo com sucesso"}

# =========================================================
# 🗑️ ROTA DELETAR PLANO (COM DESVINCULAÇÃO SEGURA)
# =========================================================
@app.delete("/api/admin/plans/{pid}")
def del_plano(pid: int, db: Session = Depends(get_db)):
    try:
        # 1. Busca o plano
        p = db.query(PlanoConfig).filter(PlanoConfig.id == pid).first()
        if not p:
            return {"status": "deleted", "msg": "Plano não existia"}

        # 2. Desvincula de Campanhas de Remarketing (Para não travar)
        db.query(RemarketingCampaign).filter(RemarketingCampaign.plano_id == pid).update(
            {RemarketingCampaign.plano_id: None}, 
            synchronize_session=False
        )

        # 3. Desvincula de Pedidos/Vendas (Para manter o histórico mas permitir deletar)
        db.query(Pedido).filter(Pedido.plano_id == pid).update(
            {Pedido.plano_id: None}, 
            synchronize_session=False
        )

        # 4. Deleta o plano
        db.delete(p)
        db.commit()
        
        return {"status": "deleted"}
        
    except Exception as e:
        logger.error(f"Erro ao deletar plano {pid}: {e}")
        raise HTTPException(status_code=400, detail=f"Erro ao deletar: {str(e)}")

# --- ROTA NOVA: ATUALIZAR PLANO ---
@app.put("/api/admin/plans/{plan_id}")
# --- ROTA NOVA: ATUALIZAR PLANO ---
@app.put("/api/admin/plans/{plan_id}")
def atualizar_plano(plan_id: int, dados: PlanoUpdate, db: Session = Depends(get_db)):
    plano = db.query(PlanoConfig).filter(PlanoConfig.id == plan_id).first()
    if not plano:
        raise HTTPException(status_code=404, detail="Plano não encontrado")
    
    # Atualiza apenas se o campo foi enviado e não é None
    if dados.nome_exibicao is not None:
        plano.nome_exibicao = dados.nome_exibicao
    
    if dados.preco is not None:
        plano.preco_atual = dados.preco
        plano.preco_cheio = dados.preco * 2 
        
    if dados.dias_duracao is not None:
        plano.dias_duracao = dados.dias_duracao
        plano.key_id = f"plan_{plano.bot_id}_{dados.dias_duracao}d"
        plano.descricao = f"Acesso de {dados.dias_duracao} dias"

    db.commit()
    db.refresh(plano)
    return {"status": "success", "msg": "Plano atualizado"}

@app.get("/api/admin/bots/{bot_id}/flow")
def obter_fluxo(bot_id: int, db: Session = Depends(get_db)):
    fluxo = db.query(BotFlow).filter(BotFlow.bot_id == bot_id).first()
    if not fluxo:
        return {
            "msg_boas_vindas": "Olá! Seja bem-vindo(a).",
            "media_url": "",
            "btn_text_1": "🔓 DESBLOQUEAR ACESSO",
            "autodestruir_1": False,
            "msg_2_texto": "Escolha seu plano abaixo:",
            "msg_2_media": "",
            "mostrar_planos_2": True,
            "mostrar_planos_1": False # Padrão
        }
    return fluxo

@app.post("/api/admin/bots/{bot_id}/flow")
def salvar_fluxo(bot_id: int, flow: FlowUpdate, db: Session = Depends(get_db)):
    fluxo_db = db.query(BotFlow).filter(BotFlow.bot_id == bot_id).first()
    if not fluxo_db:
        fluxo_db = BotFlow(bot_id=bot_id)
        db.add(fluxo_db)
    
    fluxo_db.msg_boas_vindas = flow.msg_boas_vindas
    fluxo_db.media_url = flow.media_url
    fluxo_db.btn_text_1 = flow.btn_text_1
    fluxo_db.autodestruir_1 = flow.autodestruir_1
    fluxo_db.msg_2_texto = flow.msg_2_texto
    fluxo_db.msg_2_media = flow.msg_2_media
    fluxo_db.mostrar_planos_2 = flow.mostrar_planos_2
    fluxo_db.mostrar_planos_1 = flow.mostrar_planos_1 # Salva o novo campo
    
    db.commit()
    return {"status": "saved"}

# =========================================================
# 🧩 ROTAS DE PASSOS DINÂMICOS (FLOW V2)
# =========================================================
@app.get("/api/admin/bots/{bot_id}/flow/steps")
def listar_passos_flow(bot_id: int, db: Session = Depends(get_db)):
    return db.query(BotFlowStep).filter(BotFlowStep.bot_id == bot_id).order_by(BotFlowStep.step_order).all()

@app.post("/api/admin/bots/{bot_id}/flow/steps")
def adicionar_passo_flow(bot_id: int, payload: FlowStepCreate, db: Session = Depends(get_db)):
    bot = db.query(Bot).filter(Bot.id == bot_id).first()
    if not bot: raise HTTPException(404, "Bot não encontrado")
    
    # Cria o novo passo
    novo_passo = BotFlowStep(
        bot_id=bot_id, step_order=payload.step_order,
        msg_texto=payload.msg_texto, msg_media=payload.msg_media,
        btn_texto=payload.btn_texto
    )
    db.add(novo_passo)
    db.commit()
    return {"status": "success"}

@app.put("/api/admin/bots/{bot_id}/flow/steps/{step_id}")
def atualizar_passo_flow(bot_id: int, step_id: int, dados: FlowStepUpdate, db: Session = Depends(get_db)):
    """Atualiza um passo intermediário existente"""
    passo = db.query(BotFlowStep).filter(
        BotFlowStep.id == step_id,
        BotFlowStep.bot_id == bot_id
    ).first()
    
    if not passo:
        raise HTTPException(status_code=404, detail="Passo não encontrado")
    
    # Atualiza apenas os campos enviados
    if dados.msg_texto is not None:
        passo.msg_texto = dados.msg_texto
    if dados.msg_media is not None:
        passo.msg_media = dados.msg_media
    if dados.btn_texto is not None:
        passo.btn_texto = dados.btn_texto
    if dados.autodestruir is not None:
        passo.autodestruir = dados.autodestruir
    if dados.mostrar_botao is not None:
        passo.mostrar_botao = dados.mostrar_botao
    if dados.delay_seconds is not None:
        passo.delay_seconds = dados.delay_seconds
    
    db.commit()
    db.refresh(passo)
    return {"status": "success", "passo": passo}


@app.delete("/api/admin/bots/{bot_id}/flow/steps/{sid}")
def remover_passo_flow(bot_id: int, sid: int, db: Session = Depends(get_db)):
    passo = db.query(BotFlowStep).filter(BotFlowStep.id == sid, BotFlowStep.bot_id == bot_id).first()
    if passo:
        db.delete(passo)
        db.commit()
    return {"status": "deleted"}

# =========================================================
# 💳 WEBHOOK PIX (PUSHIN PAY) - VERSÃO HTML BLINDADA
# =========================================================
@app.post("/webhook/pix")
async def webhook_pix(request: Request, db: Session = Depends(get_db)):
    print("🔔 WEBHOOK PIX CHEGOU!") 
    try:
        # 1. PEGA O CORPO BRUTO (Mais seguro contra erros de parsing)
        body_bytes = await request.body()
        body_str = body_bytes.decode("utf-8")
        
        # Tratamento de JSON ou Form Data
        try:
            data = json.loads(body_str)
            if isinstance(data, list):
                data = data[0]
        except:
            try:
                parsed = urllib.parse.parse_qs(body_str)
                data = {k: v[0] for k, v in parsed.items()}
            except:
                logger.error(f"❌ Não foi possível ler o corpo do webhook: {body_str}")
                return {"status": "ignored"}

        # 2. EXTRAÇÃO E NORMALIZAÇÃO DO ID
        raw_tx_id = data.get("id") or data.get("external_reference") or data.get("uuid")
        tx_id = str(raw_tx_id).lower() if raw_tx_id else None
        
        # Status
        status_pix = str(data.get("status", "")).lower()
        
        if status_pix not in ["paid", "approved", "completed", "succeeded"]:
            return {"status": "ignored"}

        # 3. BUSCA O PEDIDO
        pedido = db.query(Pedido).filter((Pedido.txid == tx_id) | (Pedido.transaction_id == tx_id)).first()

        if not pedido:
            print(f"❌ Pedido {tx_id} não encontrado no banco.")
            return {"status": "ok", "msg": "Order not found"}

        if pedido.status == "approved" or pedido.status == "paid":
            return {"status": "ok", "msg": "Already paid"}

        # --- 4. CÁLCULO DA DATA DE EXPIRAÇÃO ---
        now = datetime.utcnow()
        data_validade = None 
        
        # A) Pelo ID do plano
        if pedido.plano_id:
            pid = int(pedido.plano_id) if str(pedido.plano_id).isdigit() else None
            if pid:
                plano_db = db.query(PlanoConfig).filter(PlanoConfig.id == pid).first()
                if plano_db and plano_db.dias_duracao and plano_db.dias_duracao < 90000:
                    data_validade = now + timedelta(days=plano_db.dias_duracao)

        # B) Fallback pelo nome
        if not data_validade and pedido.plano_nome:
            nm = pedido.plano_nome.lower()
            if "vital" not in nm and "mega" not in nm and "eterno" not in nm:
                dias = 30 # Padrão
                if "24" in nm or "diario" in nm or "1 dia" in nm: dias = 1
                elif "semanal" in nm: dias = 7
                elif "trimestral" in nm: dias = 90
                elif "anual" in nm: dias = 365
                data_validade = now + timedelta(days=dias)

        # 5. ATUALIZA O PEDIDO
        pedido.status = "approved" 
        pedido.data_aprovacao = now
        pedido.data_expiracao = data_validade     
        pedido.custom_expiration = data_validade
        pedido.mensagem_enviada = True
        db.commit()
        
        texto_validade = data_validade.strftime("%d/%m/%Y") if data_validade else "VITALÍCIO ♾️"
        print(f"✅ Pedido {tx_id} APROVADO! Validade: {texto_validade}")
        
        # 6. ENTREGA E NOTIFICAÇÕES (EM HTML)
        try:
            bot_data = db.query(Bot).filter(Bot.id == pedido.bot_id).first()
            if bot_data:
                tb = telebot.TeleBot(bot_data.token)
                
                # --- A) ENTREGA PRODUTO PRINCIPAL ---
                try: 
                    # Limpeza e Desbanimento
                    canal_id = bot_data.id_canal_vip
                    if str(canal_id).replace("-","").isdigit():
                         canal_id = int(str(canal_id).strip())

                    try: tb.unban_chat_member(canal_id, int(pedido.telegram_id))
                    except: pass

                    # Gera Link Único
                    link_acesso = None
                    try:
                        convite = tb.create_chat_invite_link(
                            chat_id=canal_id, 
                            member_limit=1, 
                            name=f"Venda {pedido.first_name}"
                        )
                        link_acesso = convite.invite_link
                    except Exception as e_link:
                        logger.warning(f"Erro ao gerar link: {e_link}. Usando link salvo.")
                        link_acesso = pedido.link_acesso 

                    if link_acesso:
                        msg_cliente = (
                            f"✅ <b>Pagamento Confirmado!</b>\n"
                            f"📅 Validade: <b>{texto_validade}</b>\n\n"
                            f"Seu acesso exclusivo:\n👉 {link_acesso}"
                        )
                        tb.send_message(int(pedido.telegram_id), msg_cliente, parse_mode="HTML")
                    else:
                        tb.send_message(int(pedido.telegram_id), f"✅ <b>Pagamento Confirmado!</b>\nVocê já pode acessar o canal VIP.", parse_mode="HTML")

                except Exception as e_main:
                    logger.error(f"Erro na entrega principal: {e_main}")

                # --- B) ENTREGA DO ORDER BUMP (HTML) ---
                if pedido.tem_order_bump:
                    logger.info(f"🎁 [PIX] Entregando Order Bump...")
                    try:
                        bump_config = db.query(OrderBumpConfig).filter(OrderBumpConfig.bot_id == bot_data.id).first()
                        if bump_config and bump_config.link_acesso:
                            msg_bump = f"""🎁 <b>BÔNUS LIBERADO!</b>

Você também garantiu acesso ao: 
👉 <b>{bump_config.nome_produto}</b>

🔗 <b>Acesse seu conteúdo extra abaixo:</b>
{bump_config.link_acesso}"""
                            tb.send_message(int(pedido.telegram_id), msg_bump, parse_mode="HTML")
                    except Exception as e_bump:
                        logger.error(f"Erro ao entregar Bump: {e_bump}")

                # --- C) NOTIFICAÇÃO AO ADMIN (HTML) ---
                if bot_data.admin_principal_id:
                    msg_admin = (
                        f"💰 <b>VENDA NO BOT {bot_data.nome}</b>\n"
                        f"👤 {pedido.first_name} (@{pedido.username})\n"
                        f"💎 {pedido.plano_nome}\n"
                        f"💵 R$ {pedido.valor:.2f}\n"
                        f"📅 Vence em: {texto_validade}"
                    )
                    try: tb.send_message(bot_data.admin_principal_id, msg_admin, parse_mode="HTML")
                    except: print("Erro ao notificar admin")

        except Exception as e_tg:
            print(f"❌ Erro Telegram/Entrega: {e_tg}")

        return {"status": "received"}

    except Exception as e:
        print(f"❌ ERRO CRÍTICO NO WEBHOOK: {e}")
        return {"status": "error"}

# =========================================================
# 🧠 FUNÇÕES AUXILIARES DE FLUXO (RECURSIVIDADE)
# =========================================================

def enviar_oferta_final(bot_temp, chat_id, fluxo, bot_id, db):
    """Envia a oferta final (Planos) com HTML"""
    mk = types.InlineKeyboardMarkup()
    if fluxo and fluxo.mostrar_planos_2:
        planos = db.query(PlanoConfig).filter(PlanoConfig.bot_id == bot_id).all()
        for p in planos:
            mk.add(types.InlineKeyboardButton(
                f"💎 {p.nome_exibicao} - R$ {p.preco_atual:.2f}", 
                callback_data=f"checkout_{p.id}"
            ))
    
    texto = fluxo.msg_2_texto if (fluxo and fluxo.msg_2_texto) else "Confira nossos planos:"
    media = fluxo.msg_2_media if fluxo else None
    
    try:
        if media:
            if media.lower().endswith(('.mp4', '.mov', '.avi')): 
                # 🔥 parse_mode="HTML"
                bot_temp.send_video(chat_id, media, caption=texto, reply_markup=mk, parse_mode="HTML")
            else: 
                # 🔥 parse_mode="HTML"
                bot_temp.send_photo(chat_id, media, caption=texto, reply_markup=mk, parse_mode="HTML")
        else:
            # 🔥 parse_mode="HTML"
            bot_temp.send_message(chat_id, texto, reply_markup=mk, parse_mode="HTML")
            
    except Exception as e:
        logger.error(f"Erro ao enviar oferta final: {e}")
        # Fallback sem HTML
        bot_temp.send_message(chat_id, texto, reply_markup=mk)

def enviar_passo_automatico(bot_temp, chat_id, passo_atual, bot_db, db):
    """
    Envia um passo e, se não tiver botão e tiver delay, 
    agenda e envia o PRÓXIMO (ou a oferta) automaticamente.
    """
    try:
        # 1. Configura botão se houver
        markup_step = types.InlineKeyboardMarkup()
        if passo_atual.mostrar_botao:
            # Verifica se existe um PRÓXIMO passo depois deste
            prox = db.query(BotFlowStep).filter(
                BotFlowStep.bot_id == bot_db.id, 
                BotFlowStep.step_order == passo_atual.step_order + 1
            ).first()
            
            callback = f"next_step_{passo_atual.step_order}" if prox else "go_checkout"
            markup_step.add(types.InlineKeyboardButton(text=passo_atual.btn_texto, callback_data=callback))

        # 2. Envia a mensagem deste passo
        sent_msg = None
        if passo_atual.msg_media:
            try:
                if passo_atual.msg_media.lower().endswith(('.mp4', '.mov')):
                    sent_msg = bot_temp.send_video(chat_id, passo_atual.msg_media, caption=passo_atual.msg_texto, reply_markup=markup_step if passo_atual.mostrar_botao else None)
                else:
                    sent_msg = bot_temp.send_photo(chat_id, passo_atual.msg_media, caption=passo_atual.msg_texto, reply_markup=markup_step if passo_atual.mostrar_botao else None)
            except:
                sent_msg = bot_temp.send_message(chat_id, passo_atual.msg_texto, reply_markup=markup_step if passo_atual.mostrar_botao else None)
        else:
            sent_msg = bot_temp.send_message(chat_id, passo_atual.msg_texto, reply_markup=markup_step if passo_atual.mostrar_botao else None)

        # 3. Lógica Automática (Sem botão + Delay)
        if not passo_atual.mostrar_botao and passo_atual.delay_seconds > 0:
            logger.info(f"⏳ [BOT {bot_db.id}] Passo {passo_atual.step_order}: Aguardando {passo_atual.delay_seconds}s...")
            time.sleep(passo_atual.delay_seconds)
            
            # Auto-destruir este passo (se configurado)
            if passo_atual.autodestruir and sent_msg:
                try:
                    bot_temp.delete_message(chat_id, sent_msg.message_id)
                except: pass
            
            # 🔥 DECISÃO: Chama o próximo passo OU a Oferta Final
            proximo_passo = db.query(BotFlowStep).filter(
                BotFlowStep.bot_id == bot_db.id, 
                BotFlowStep.step_order == passo_atual.step_order + 1
            ).first()
            
            if proximo_passo:
                enviar_passo_automatico(bot_temp, chat_id, proximo_passo, bot_db, db)
            else:
                # FIM DA LINHA -> Manda Oferta
                enviar_oferta_final(bot_temp, chat_id, bot_db.fluxo, bot_db.id, db)

    except Exception as e:
        logger.error(f"Erro no passo automático {passo_atual.step_order}: {e}")
# =========================================================
# 🚀 WEBHOOK GERAL (PORTEIRO + ORDER BUMP + FLUXO + HTML + PROMO FULL)
# =========================================================
# =========================================================
# 🚀 WEBHOOK GERAL (TEXTOS CORRIGIDOS + PORTEIRO + HTML)
# =========================================================
@app.post("/webhook/{token}")
async def receber_update_telegram(token: str, req: Request, db: Session = Depends(get_db)):
    
    # 1. Proteção contra Loop do PIX
    if token == "pix": return {"status": "ignored"}
    
    # 2. Verifica se o Bot existe e está ativo
    bot_db = db.query(Bot).filter(Bot.token == token).first()
    if not bot_db or bot_db.status == "pausado": 
        return {"status": "ignored"}

    try:
        # Lê o update do Telegram
        body = await req.json()
        update = telebot.types.Update.de_json(body)
        bot_temp = telebot.TeleBot(token)
        
        # Define message com segurança
        message = update.message if update.message else None
        
        # ============================================================
        # 🚪 1. O PORTEIRO (GATEKEEPER)
        # ============================================================
        if message and message.new_chat_members:
            chat_id = str(message.chat.id)
            canal_vip_id = str(bot_db.id_canal_vip).replace(" ", "").strip()
            
            if chat_id == canal_vip_id:
                for member in message.new_chat_members:
                    if member.is_bot: continue
                    
                    # Busca pedido pago
                    pedido = db.query(Pedido).filter(
                        Pedido.bot_id == bot_db.id,
                        Pedido.telegram_id == str(member.id),
                        Pedido.status.in_(['paid', 'approved'])
                    ).order_by(desc(Pedido.created_at)).first()
                    
                    allowed = False
                    if pedido:
                        if pedido.data_expiracao:
                            if datetime.utcnow() < pedido.data_expiracao: allowed = True
                        elif pedido.plano_nome:
                            nm = pedido.plano_nome.lower()
                            if "vital" in nm or "mega" in nm or "eterno" in nm: allowed = True
                            else:
                                d = 30
                                if "diario" in nm or "24" in nm: d = 1
                                elif "semanal" in nm: d = 7
                                elif "trimestral" in nm: d = 90
                                elif "anual" in nm: d = 365
                                if pedido.created_at and datetime.utcnow() < (pedido.created_at + timedelta(days=d)):
                                    allowed = True
                    
                    if not allowed:
                        try:
                            bot_temp.ban_chat_member(chat_id, member.id)
                            bot_temp.unban_chat_member(chat_id, member.id)
                            try:
                                bot_temp.send_message(member.id, "🚫 <b>Acesso Negado.</b>\nPor favor, realize o pagamento para entrar.", parse_mode="HTML")
                            except: pass
                        except: pass
            
            return {"status": "checked"}

        # ============================================================
        # 👋 2. COMANDO /START
        # ============================================================
        if message and message.text and (message.text == "/start" or message.text.startswith("/start ")):
            chat_id = message.chat.id
            first_name = message.from_user.first_name
            username = message.from_user.username
            
            # --- SALVA LEAD ---
            try:
                lead = db.query(Lead).filter(Lead.user_id == str(chat_id), Lead.bot_id == bot_db.id).first()
                if not lead:
                    lead = Lead(user_id=str(chat_id), nome=first_name, username=username, bot_id=bot_db.id, status="topo", funil_stage="lead_frio", created_at=datetime.utcnow())
                    db.add(lead)
                else:
                    lead.nome = first_name
                    lead.username = username
                    lead.ultimo_contato = datetime.utcnow()
                db.commit()
            except: pass

            # --- CARREGA FLUXO ---
            fluxo = db.query(BotFlow).filter(BotFlow.bot_id == bot_db.id).first()
            msg_texto = fluxo.msg_boas_vindas if fluxo else "Olá! Seja bem-vindo."
            msg_media = fluxo.media_url if fluxo else None
            mostrar_planos_1 = fluxo.mostrar_planos_1 if fluxo else False

            markup = types.InlineKeyboardMarkup()
            
            if mostrar_planos_1:
                planos = db.query(PlanoConfig).filter(PlanoConfig.bot_id == bot_db.id).all()
                for p in planos:
                    markup.add(types.InlineKeyboardButton(f"💎 {p.nome_exibicao} - R$ {p.preco_atual:.2f}", callback_data=f"checkout_{p.id}"))
            else:
                btn_txt = fluxo.btn_text_1 if (fluxo and fluxo.btn_text_1) else "🔓 VER CONTEÚDO"
                markup.add(types.InlineKeyboardButton(btn_txt, callback_data="step_1"))

            # Envia (HTML)
            try:
                if msg_media:
                    if msg_media.lower().endswith(('.mp4', '.mov')):
                        bot_temp.send_video(chat_id, msg_media, caption=msg_texto, reply_markup=markup, parse_mode="HTML")
                    else:
                        bot_temp.send_photo(chat_id, msg_media, caption=msg_texto, reply_markup=markup, parse_mode="HTML")
                else:
                    bot_temp.send_message(chat_id, msg_texto, reply_markup=markup, parse_mode="HTML")
            except:
                bot_temp.send_message(chat_id, msg_texto, reply_markup=markup) 

            return {"status": "ok"}

        # ============================================================
        # 🎮 3. CALLBACKS (BOTÕES)
        # ============================================================
        elif update.callback_query:
            try: bot_temp.answer_callback_query(update.callback_query.id)
            except: pass
            
            chat_id = update.callback_query.message.chat.id
            data = update.callback_query.data
            first_name = update.callback_query.from_user.first_name
            username = update.callback_query.from_user.username

            # --- A) NAVEGAÇÃO (step_) ---
            if data.startswith("step_"):
                try: current_step = int(data.split("_")[1])
                except: current_step = 1
                
                steps = db.query(BotFlowStep).filter(BotFlowStep.bot_id == bot_db.id).order_by(BotFlowStep.step_order).all()
                
                target_step = None
                is_last = False
                if current_step <= len(steps):
                    target_step = steps[current_step - 1]
                else:
                    is_last = True

                if target_step and not is_last:
                    mk = types.InlineKeyboardMarkup()
                    if target_step.mostrar_botao:
                        mk.add(types.InlineKeyboardButton(target_step.btn_texto or "Próximo ▶️", callback_data=f"step_{current_step + 1}"))
                    
                    sent_msg = None
                    try:
                        if target_step.msg_media:
                            if target_step.msg_media.lower().endswith(('.mp4', '.mov')):
                                sent_msg = bot_temp.send_video(chat_id, target_step.msg_media, caption=target_step.msg_texto, reply_markup=mk, parse_mode="HTML")
                            else:
                                sent_msg = bot_temp.send_photo(chat_id, target_step.msg_media, caption=target_step.msg_texto, reply_markup=mk, parse_mode="HTML")
                        else:
                            sent_msg = bot_temp.send_message(chat_id, target_step.msg_texto, reply_markup=mk, parse_mode="HTML")
                    except:
                        sent_msg = bot_temp.send_message(chat_id, target_step.msg_texto or "...", reply_markup=mk)

                    # Delay Automático
                    if not target_step.mostrar_botao and target_step.delay_seconds > 0:
                        time.sleep(target_step.delay_seconds)
                        if target_step.autodestruir and sent_msg:
                            try: bot_temp.delete_message(chat_id, sent_msg.message_id)
                            except: pass
                        
                        prox = db.query(BotFlowStep).filter(BotFlowStep.bot_id == bot_db.id, BotFlowStep.step_order == target_step.step_order + 1).first()
                        if prox:
                            enviar_passo_automatico(bot_temp, chat_id, prox, bot_db, db)
                        else:
                            enviar_oferta_final(bot_temp, chat_id, bot_db.fluxo, bot_db.id, db)
                else:
                    enviar_oferta_final(bot_temp, chat_id, bot_db.fluxo, bot_db.id, db)

            # --- B) CHECKOUT (MENSAGEM CORRIGIDA) ---
            elif data.startswith("checkout_"):
                plano_id = data.split("_")[1]
                plano = db.query(PlanoConfig).filter(PlanoConfig.id == plano_id).first()
                if not plano: return {"status": "error"}

                bump = db.query(OrderBumpConfig).filter(OrderBumpConfig.bot_id == bot_db.id, OrderBumpConfig.ativo == True).first()
                
                if bump:
                    # Manda Order Bump
                    mk = types.InlineKeyboardMarkup()
                    mk.row(
                        types.InlineKeyboardButton(f"{bump.btn_aceitar} (+ R$ {bump.preco:.2f})", callback_data=f"bump_yes_{plano.id}"),
                        types.InlineKeyboardButton(bump.btn_recusar, callback_data=f"bump_no_{plano.id}")
                    )
                    txt_bump = bump.msg_texto or f"Levar {bump.nome_produto} junto?"
                    try:
                        if bump.msg_media:
                            if bump.msg_media.lower().endswith(('.mp4','.mov')):
                                bot_temp.send_video(chat_id, bump.msg_media, caption=txt_bump, reply_markup=mk, parse_mode="HTML")
                            else:
                                bot_temp.send_photo(chat_id, bump.msg_media, caption=txt_bump, reply_markup=mk, parse_mode="HTML")
                        else:
                            bot_temp.send_message(chat_id, txt_bump, reply_markup=mk, parse_mode="HTML")
                    except:
                        bot_temp.send_message(chat_id, txt_bump, reply_markup=mk, parse_mode="HTML")
                else:
                    # Gera PIX direto
                    msg_wait = bot_temp.send_message(chat_id, "⏳ Gerando <b>PIX</b>...", parse_mode="HTML")
                    mytx = str(uuid.uuid4())
                    pix = gerar_pix_pushinpay(plano.preco_atual, mytx)
                    
                    if pix:
                        qr = pix.get('qr_code_text') or pix.get('qr_code')
                        txid = str(pix.get('id') or mytx).lower()
                        
                        novo_pedido = Pedido(
                            bot_id=bot_db.id, telegram_id=str(chat_id), first_name=first_name, username=username,
                            plano_nome=plano.nome_exibicao, plano_id=plano.id, valor=plano.preco_atual,
                            transaction_id=txid, qr_code=qr, status="pending", tem_order_bump=False, created_at=datetime.utcnow()
                        )
                        db.add(novo_pedido)
                        db.commit()
                        
                        try: bot_temp.delete_message(chat_id, msg_wait.message_id)
                        except: pass
                        
                        # 🔥 MENSAGEM PADRÃO CORRIGIDA
                        msg_pix = f"""🌟 Seu pagamento foi gerado com sucesso:
🎁 Plano: <b>{plano.nome_exibicao}</b>
💰 Valor: <b>R$ {plano.preco_atual:.2f}</b>
🔐 Pague via Pix Copia e Cola:

<pre>{qr}</pre>

👆 Toque na chave PIX acima para copiá-la
‼️ Após o pagamento, o acesso será liberado automaticamente!"""
                        
                        bot_temp.send_message(chat_id, msg_pix, parse_mode="HTML")
                    else:
                        bot_temp.send_message(chat_id, "❌ Erro ao gerar PIX.")

            # --- C) BUMP YES/NO (MENSAGEM CORRIGIDA) ---
            elif data.startswith("bump_yes_") or data.startswith("bump_no_"):
                aceitou = "yes" in data
                pid = data.split("_")[2]
                plano = db.query(PlanoConfig).filter(PlanoConfig.id == pid).first()
                bump = db.query(OrderBumpConfig).filter(OrderBumpConfig.bot_id == bot_db.id).first()
                
                if bump and bump.autodestruir:
                    try: bot_temp.delete_message(chat_id, update.callback_query.message.message_id)
                    except: pass
                
                valor_final = plano.preco_atual
                nome_final = plano.nome_exibicao
                if aceitou and bump:
                    valor_final += bump.preco
                    nome_final += f" + {bump.nome_produto}"
                
                msg_wait = bot_temp.send_message(chat_id, f"⏳ Gerando PIX: <b>{nome_final}</b>...", parse_mode="HTML")
                mytx = str(uuid.uuid4())
                pix = gerar_pix_pushinpay(valor_final, mytx)
                
                if pix:
                    qr = pix.get('qr_code_text') or pix.get('qr_code')
                    txid = str(pix.get('id') or mytx).lower()
                    
                    novo_pedido = Pedido(
                        bot_id=bot_db.id, telegram_id=str(chat_id), first_name=first_name, username=username,
                        plano_nome=nome_final, plano_id=plano.id, valor=valor_final,
                        transaction_id=txid, qr_code=qr, status="pending", tem_order_bump=aceitou, created_at=datetime.utcnow()
                    )
                    db.add(novo_pedido)
                    db.commit()
                    
                    try: bot_temp.delete_message(chat_id, msg_wait.message_id)
                    except: pass
                    
                    # 🔥 MENSAGEM COMBO CORRIGIDA
                    msg_pix = f"""🌟 Seu pagamento foi gerado com sucesso:
🎁 Plano: <b>{nome_final}</b>
💰 Valor: <b>R$ {valor_final:.2f}</b>
🔐 Pague via Pix Copia e Cola:

<pre>{qr}</pre>

👆 Toque na chave PIX acima para copiá-la
‼️ Após o pagamento, o acesso será liberado automaticamente!"""

                    bot_temp.send_message(chat_id, msg_pix, parse_mode="HTML")

            # --- D) PROMOÇÕES (MENSAGEM CORRIGIDA) ---
            elif data.startswith("promo_"):
                try: campanha_uuid = data.split("_")[1]
                except: campanha_uuid = ""
                
                logger.info(f"🔍 [WEBHOOK] Promo ativada: {campanha_uuid} por {chat_id}")
                
                campanha = db.query(RemarketingCampaign).filter(RemarketingCampaign.campaign_id == campanha_uuid).first()
                
                if not campanha:
                    bot_temp.send_message(chat_id, "❌ Oferta não encontrada ou expirada.")
                
                elif campanha.expiration_at and datetime.utcnow() > campanha.expiration_at:
                    bot_temp.send_message(chat_id, "🚫 <b>OFERTA ENCERRADA!</b>\n\nO tempo desta oferta acabou.", parse_mode="HTML")
                
                else:
                    plano = db.query(PlanoConfig).filter(PlanoConfig.id == campanha.plano_id).first()
                    if plano:
                        preco_final = campanha.promo_price if campanha.promo_price else plano.preco_atual
                        
                        msg_wait = bot_temp.send_message(chat_id, "⏳ Gerando <b>OFERTA ESPECIAL</b>...", parse_mode="HTML")
                        mytx = str(uuid.uuid4())
                        pix = gerar_pix_pushinpay(preco_final, mytx)
                        
                        if pix:
                            qr = pix.get('qr_code_text') or pix.get('qr_code')
                            txid = str(pix.get('id') or mytx).lower()
                            
                            novo_pedido = Pedido(
                                bot_id=bot_db.id, telegram_id=str(chat_id), first_name=first_name, username=username,
                                plano_nome=f"{plano.nome_exibicao} (OFERTA)", plano_id=plano.id, valor=preco_final,
                                transaction_id=txid, qr_code=qr, status="pending", tem_order_bump=False, created_at=datetime.utcnow()
                            )
                            db.add(novo_pedido)
                            db.commit()
                            
                            try: bot_temp.delete_message(chat_id, msg_wait.message_id)
                            except: pass
                            
                            # 🔥 MENSAGEM PROMO CORRIGIDA
                            msg_pix = f"""🌟 Seu pagamento foi gerado com sucesso:
🎁 Plano: <b>{plano.nome_exibicao}</b>
💰 Valor Promocional: <b>R$ {preco_final:.2f}</b>
🔐 Pague via Pix Copia e Cola:

<pre>{qr}</pre>

👆 Toque na chave PIX acima para copiá-la
‼️ Após o pagamento, o acesso será liberado automaticamente!"""

                            bot_temp.send_message(chat_id, msg_pix, parse_mode="HTML")
                        else:
                            bot_temp.send_message(chat_id, "❌ Erro ao gerar PIX da oferta.")
                    else:
                        bot_temp.send_message(chat_id, "❌ Plano da oferta não encontrado.")

    except Exception as e:
        logger.error(f"Erro no webhook: {e}")

    return {"status": "ok"}

# ============================================================
# ROTA 1: LISTAR LEADS (TOPO DO FUNIL)
# ============================================================
@app.get("/api/admin/leads")
def listar_leads(
    bot_id: Optional[int] = None,
    page: int = 1,
    per_page: int = 50,
    db: Session = Depends(get_db)
):
    """
    Lista leads (usuários que só deram /start)
    """
    try:
        # Query base
        query = db.query(Lead)
        
        # Filtro por bot
        if bot_id:
            query = query.filter(Lead.bot_id == bot_id)
        
        # Contagem total
        total = query.count()
        
        # Paginação
        offset = (page - 1) * per_page
        leads = query.order_by(Lead.created_at.desc()).offset(offset).limit(per_page).all()
        
        # Formata resposta
        leads_data = []
        for lead in leads:
            leads_data.append({
                "id": lead.id,
                "user_id": lead.user_id,
                "nome": lead.nome,
                "username": lead.username,
                "bot_id": lead.bot_id,
                "status": lead.status,
                "funil_stage": lead.funil_stage,
                "primeiro_contato": lead.primeiro_contato.isoformat() if lead.primeiro_contato else None,
                "ultimo_contato": lead.ultimo_contato.isoformat() if lead.ultimo_contato else None,
                "total_remarketings": lead.total_remarketings,
                "ultimo_remarketing": lead.ultimo_remarketing.isoformat() if lead.ultimo_remarketing else None,
                "created_at": lead.created_at.isoformat() if lead.created_at else None
            })
        
        return {
            "data": leads_data,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": (total + per_page - 1) // per_page
        }
    
    except Exception as e:
        logger.error(f"Erro ao listar leads: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# ROTA 2: ESTATÍSTICAS DO FUNIL
# ============================================================
# ============================================================
# 🔥 ROTA ATUALIZADA: /api/admin/contacts/funnel-stats
# SUBSTITUA a rota existente por esta versão
# Calcula estatísticas baseando-se no campo 'status' (não status_funil)
# ============================================================

@app.get("/api/admin/contacts/funnel-stats")
def obter_estatisticas_funil(
    bot_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """
    🔥 [CORRIGIDO] Retorna contadores de cada estágio do funil
    TOPO = Leads (tabela Lead)
    MEIO = Pedidos com status 'pending' (gerou PIX mas não pagou)
    FUNDO = Pedidos com status 'paid/active/approved' (pagou)
    EXPIRADO = Pedidos com status 'expired'
    """
    try:
        # ============================================================
        # TOPO: Contar LEADS (tabela Lead)
        # ============================================================
        query_topo = db.query(Lead)
        if bot_id:
            query_topo = query_topo.filter(Lead.bot_id == bot_id)
        topo = query_topo.count()
        
        # ============================================================
        # MEIO: Pedidos com status PENDING (gerou PIX, não pagou)
        # ============================================================
        query_meio = db.query(Pedido).filter(Pedido.status == 'pending')
        if bot_id:
            query_meio = query_meio.filter(Pedido.bot_id == bot_id)
        meio = query_meio.count()
        
        # ============================================================
        # FUNDO: Pedidos PAGOS (paid/active/approved)
        # ============================================================
        query_fundo = db.query(Pedido).filter(
            Pedido.status.in_(['paid', 'active', 'approved'])
        )
        if bot_id:
            query_fundo = query_fundo.filter(Pedido.bot_id == bot_id)
        fundo = query_fundo.count()
        
        # ============================================================
        # EXPIRADOS: Pedidos com status EXPIRED
        # ============================================================
        query_expirados = db.query(Pedido).filter(Pedido.status == 'expired')
        if bot_id:
            query_expirados = query_expirados.filter(Pedido.bot_id == bot_id)
        expirados = query_expirados.count()
        
        # Total
        total = topo + meio + fundo + expirados
        
        logger.info(f"📊 Estatísticas do funil: TOPO={topo}, MEIO={meio}, FUNDO={fundo}, EXPIRADOS={expirados}")
        
        return {
            "topo": topo,
            "meio": meio,
            "fundo": fundo,
            "expirados": expirados,
            "total": total
        }
    
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas do funil: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# ROTA 3: ATUALIZAR ROTA DE CONTATOS EXISTENTE
# ============================================================
# Procure a rota @app.get("/api/admin/contacts") no seu main.py
# e SUBSTITUA por esta versão atualizada:

# ============================================================
# 🔥 ROTA ATUALIZADA: /api/admin/contacts
# SUBSTITUA a rota existente por esta versão
# ADICIONA SUPORTE PARA FILTROS: meio, fundo, expirado
# ============================================================

@app.get("/api/admin/contacts")
async def get_contacts(
    status: str = "todos",
    bot_id: Optional[int] = None,
    page: int = 1,
    per_page: int = 50,
    db: Session = Depends(get_db)
):
    """
    🔥 [ATUALIZADO] Retorna contatos únicos (sem duplicatas)
    NOVOS FILTROS: meio, fundo, expirado
    """
    try:
        offset = (page - 1) * per_page
        all_contacts = []
        
        # ============================================================
        # FILTRO: TODOS
        # ============================================================
        if status == "todos":
            contatos_unicos = {}
            
            # 1. Buscar LEADS
            query_leads = db.query(Lead)
            if bot_id:
                query_leads = query_leads.filter(Lead.bot_id == bot_id)
            leads = query_leads.all()
            logger.info(f"📊 Busca TODOS: {len(leads)} leads encontrados")
            
            for lead in leads:
                telegram_id = str(lead.user_id)
                contatos_unicos[telegram_id] = {
                    "id": lead.id,
                    "telegram_id": telegram_id,
                    "user_id": telegram_id,
                    "first_name": lead.nome or "Sem nome",
                    "nome": lead.nome or "Sem nome",
                    "username": lead.username or "sem_username",
                    "plano_nome": "-",
                    "valor": 0.0,
                    "status": "pending",
                    "role": "user",
                    "custom_expiration": None,
                    "created_at": lead.created_at or datetime.utcnow(),
                    "origem": "lead",
                    "status_funil": "topo"
                }
            
            # 2. Buscar PEDIDOS - SOBRESCREVE leads
            query_pedidos = db.query(Pedido)
            if bot_id:
                query_pedidos = query_pedidos.filter(Pedido.bot_id == bot_id)
            pedidos = query_pedidos.all()
            logger.info(f"📊 Busca TODOS: {len(pedidos)} pedidos encontrados")
            
            for pedido in pedidos:
                telegram_id = str(pedido.telegram_id)
                
                # Determina status_funil baseado no status do pagamento
                if pedido.status in ["paid", "active", "approved"]:
                    status_funil = "fundo"
                elif pedido.status == "pending":
                    status_funil = "meio"
                elif pedido.status == "expired":
                    status_funil = "expirado"
                else:
                    status_funil = "meio"
                
                contatos_unicos[telegram_id] = {
                    "id": pedido.id,
                    "telegram_id": telegram_id,
                    "user_id": telegram_id,
                    "first_name": pedido.first_name or "Sem nome",
                    "nome": pedido.first_name or "Sem nome",
                    "username": pedido.username or "sem_username",
                    "plano_nome": pedido.plano_nome or "-",
                    "valor": pedido.valor or 0.0,
                    "status": pedido.status,
                    "role": "user",
                    "custom_expiration": pedido.custom_expiration,
                    "created_at": pedido.created_at or datetime.utcnow(),
                    "origem": "pedido",
                    "status_funil": status_funil
                }
            
            all_contacts = list(contatos_unicos.values())
            logger.info(f"📊 Busca TODOS: {len(all_contacts)} contatos ÚNICOS (sem duplicatas)")
            
            all_contacts.sort(key=lambda x: x["created_at"], reverse=True)
            total = len(all_contacts)
            pag_contacts = all_contacts[offset:offset + per_page]
            
            return {
                "data": pag_contacts,
                "total": total,
                "page": page,
                "per_page": per_page,
                "total_pages": (total + per_page - 1) // per_page
            }
        
        # ============================================================
        # 🔥 [NOVO] FILTRO: MEIO (Leads Quentes - PIX gerado mas não pago)
        # ============================================================
        elif status == "meio":
            query = db.query(Pedido).filter(Pedido.status == "pending")
            if bot_id:
                query = query.filter(Pedido.bot_id == bot_id)
            
            total = query.count()
            pedidos = query.offset(offset).limit(per_page).all()
            
            logger.info(f"🔥 MEIO: {total} leads quentes (pending)")
            
            contacts = [{
                "id": p.id,
                "telegram_id": p.telegram_id,
                "first_name": p.first_name,
                "username": p.username,
                "plano_nome": p.plano_nome,
                "valor": p.valor,
                "status": p.status,
                "role": "user",
                "custom_expiration": p.custom_expiration,
                "created_at": p.created_at,
                "status_funil": "meio"
            } for p in pedidos]
            
            return {
                "data": contacts,
                "total": total,
                "page": page,
                "per_page": per_page,
                "total_pages": (total + per_page - 1) // per_page
            }
        
        # ============================================================
        # 🔥 [NOVO] FILTRO: FUNDO (Clientes - Pagos)
        # ============================================================
        elif status == "fundo":
            query = db.query(Pedido).filter(Pedido.status.in_(["paid", "active", "approved"]))
            if bot_id:
                query = query.filter(Pedido.bot_id == bot_id)
            
            total = query.count()
            pedidos = query.offset(offset).limit(per_page).all()
            
            logger.info(f"✅ FUNDO: {total} clientes pagos")
            
            contacts = [{
                "id": p.id,
                "telegram_id": p.telegram_id,
                "first_name": p.first_name,
                "username": p.username,
                "plano_nome": p.plano_nome,
                "valor": p.valor,
                "status": p.status,
                "role": "user",
                "custom_expiration": p.custom_expiration,
                "created_at": p.created_at,
                "status_funil": "fundo"
            } for p in pedidos]
            
            return {
                "data": contacts,
                "total": total,
                "page": page,
                "per_page": per_page,
                "total_pages": (total + per_page - 1) // per_page
            }
        
        # ============================================================
        # 🔥 [ATUALIZADO] FILTRO: EXPIRADO
        # ============================================================
        elif status == "expirado" or status == "expirados":
            query = db.query(Pedido).filter(Pedido.status == "expired")
            if bot_id:
                query = query.filter(Pedido.bot_id == bot_id)
            
            total = query.count()
            pedidos = query.offset(offset).limit(per_page).all()
            
            logger.info(f"❄️ EXPIRADOS: {total} pedidos expirados")
            
            contacts = [{
                "id": p.id,
                "telegram_id": p.telegram_id,
                "first_name": p.first_name,
                "username": p.username,
                "plano_nome": p.plano_nome,
                "valor": p.valor,
                "status": p.status,
                "role": "user",
                "custom_expiration": p.custom_expiration,
                "created_at": p.created_at,
                "status_funil": "expirado"
            } for p in pedidos]
            
            return {
                "data": contacts,
                "total": total,
                "page": page,
                "per_page": per_page,
                "total_pages": (total + per_page - 1) // per_page
            }
        
        # ============================================================
        # FILTRO: PAGANTES (mantido para compatibilidade)
        # ============================================================
        elif status == "pagantes":
            query = db.query(Pedido).filter(Pedido.status.in_(["paid", "active", "approved"]))
            if bot_id:
                query = query.filter(Pedido.bot_id == bot_id)
            
            total = query.count()
            pedidos = query.offset(offset).limit(per_page).all()
            
            contacts = [{
                "id": p.id,
                "telegram_id": p.telegram_id,
                "first_name": p.first_name,
                "username": p.username,
                "plano_nome": p.plano_nome,
                "valor": p.valor,
                "status": p.status,
                "role": "user",
                "custom_expiration": p.custom_expiration,
                "created_at": p.created_at
            } for p in pedidos]
            
            return {
                "data": contacts,
                "total": total,
                "page": page,
                "per_page": per_page,
                "total_pages": (total + per_page - 1) // per_page
            }
        
        # ============================================================
        # FILTRO: PENDENTES (mantido para compatibilidade)
        # ============================================================
        elif status == "pendentes":
            query = db.query(Pedido).filter(Pedido.status == "pending")
            if bot_id:
                query = query.filter(Pedido.bot_id == bot_id)
            
            total = query.count()
            pedidos = query.offset(offset).limit(per_page).all()
            
            contacts = [{
                "id": p.id,
                "telegram_id": p.telegram_id,
                "first_name": p.first_name,
                "username": p.username,
                "plano_nome": p.plano_nome,
                "valor": p.valor,
                "status": p.status,
                "role": "user",
                "custom_expiration": p.custom_expiration,
                "created_at": p.created_at
            } for p in pedidos]
            
            return {
                "data": contacts,
                "total": total,
                "page": page,
                "per_page": per_page,
                "total_pages": (total + per_page - 1) // per_page
            }
        
        else:
            return {"data": [], "total": 0, "page": 1, "per_page": per_page, "total_pages": 0}
            
    except Exception as e:
        logger.error(f"Erro ao buscar contatos: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# 🔥 ROTAS COMPLETAS - Adicione no main.py
# LOCAL: Após as rotas de /api/admin/contacts (linha ~2040)
# ============================================================

# ============================================================
# ROTA 1: Atualizar Usuário (UPDATE)
# ============================================================
@app.put("/api/admin/users/{user_id}")
async def update_user(user_id: int, data: dict, db: Session = Depends(get_db)):
    """
    ✏️ Atualiza informações de um usuário (status, role, custom_expiration)
    """
    try:
        # 1. Buscar pedido
        pedido = db.query(Pedido).filter(Pedido.id == user_id).first()
        
        if not pedido:
            logger.error(f"❌ Pedido {user_id} não encontrado")
            raise HTTPException(status_code=404, detail="Pedido não encontrado")
        
        # 2. Atualizar campos
        if "status" in data:
            pedido.status = data["status"]
            logger.info(f"✅ Status atualizado para: {data['status']}")
        
        if "role" in data:
            pedido.role = data["role"]
            logger.info(f"✅ Role atualizado para: {data['role']}")
        
        if "custom_expiration" in data:
            if data["custom_expiration"] == "remover" or data["custom_expiration"] == "":
                pedido.custom_expiration = None
                logger.info(f"✅ Data de expiração removida (Vitalício)")
            else:
                # Converter string para datetime
                try:
                    pedido.custom_expiration = datetime.strptime(data["custom_expiration"], "%Y-%m-%d")
                    logger.info(f"✅ Data de expiração atualizada: {data['custom_expiration']}")
                except:
                    # Se já for datetime, usa direto
                    pedido.custom_expiration = data["custom_expiration"]
        
        # 3. Salvar no banco
        db.commit()
        db.refresh(pedido)
        
        logger.info(f"✅ Usuário {user_id} atualizado com sucesso!")
        
        return {
            "status": "success",
            "message": "Usuário atualizado com sucesso!",
            "data": {
                "id": pedido.id,
                "telegram_id": pedido.telegram_id,
                "status": pedido.status,
                "role": pedido.role,
                "custom_expiration": pedido.custom_expiration.isoformat() if pedido.custom_expiration else None
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar usuário: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# ROTA 2: Reenviar Acesso
# ============================================================
@app.post("/api/admin/users/{user_id}/resend-access")
async def resend_user_access(user_id: int, db: Session = Depends(get_db)):
    """
    🔑 Reenvia o link de acesso VIP para um usuário que já pagou
    """
    try:
        # 1. Buscar pedido
        pedido = db.query(Pedido).filter(Pedido.id == user_id).first()
        
        if not pedido:
            logger.error(f"❌ Pedido {user_id} não encontrado")
            raise HTTPException(status_code=404, detail="Pedido não encontrado")
        
        # 2. Verificar se está pago
        if pedido.status not in ["paid", "active", "approved"]:
            logger.error(f"❌ Pedido {user_id} não está pago (status: {pedido.status})")
            raise HTTPException(
                status_code=400, 
                detail="Pedido não está pago. Altere o status para 'Ativo/Pago' primeiro."
            )
        
        # 3. Buscar bot
        bot_data = db.query(Bot).filter(Bot.id == pedido.bot_id).first()
        
        if not bot_data:
            logger.error(f"❌ Bot {pedido.bot_id} não encontrado")
            raise HTTPException(status_code=404, detail="Bot não encontrado")
        
        # 4. Verificar se bot tem canal configurado
        if not bot_data.id_canal_vip:
            logger.error(f"❌ Bot {pedido.bot_id} não tem canal VIP configurado")
            raise HTTPException(status_code=400, detail="Bot não tem canal VIP configurado")
        
        # 5. Gerar novo link e enviar
        try:
            tb = telebot.TeleBot(bot_data.token)
            
            # Tratamento do ID do Canal
            try: 
                canal_id = int(str(bot_data.id_canal_vip).strip())
            except: 
                canal_id = bot_data.id_canal_vip
            
            # Tenta desbanir antes (caso tenha sido banido)
            try:
                tb.unban_chat_member(canal_id, int(pedido.telegram_id))
                logger.info(f"🔓 Usuário {pedido.telegram_id} desbanido do canal")
            except Exception as e:
                logger.warning(f"⚠️ Não foi possível desbanir usuário: {e}")
            
            # Gera Link Único
            convite = tb.create_chat_invite_link(
                chat_id=canal_id,
                member_limit=1,
                name=f"Reenvio {pedido.first_name}"
            )
            
            # Formata data de validade
            texto_validade = "VITALÍCIO ♾️"
            if pedido.custom_expiration:
                texto_validade = pedido.custom_expiration.strftime("%d/%m/%Y")
            
            # Envia mensagem
            msg_cliente = (
                f"✅ <b>Acesso Reenviado!</b>\n"
                f"📅 Validade: <b>{texto_validade}</b>\n\n"
                f"Seu acesso exclusivo:\n👉 {convite.invite_link}\n\n"
                f"<i>Use este link para entrar no grupo VIP.</i>"
            )
            
            tb.send_message(int(pedido.telegram_id), msg_cliente, parse_mode="HTML")
            
            logger.info(f"✅ Acesso reenviado para {pedido.first_name} (ID: {pedido.telegram_id})")
            
            return {
                "status": "success",
                "message": "Acesso reenviado com sucesso!",
                "telegram_id": pedido.telegram_id,
                "nome": pedido.first_name,
                "validade": texto_validade
            }
            
        except telebot.apihelper.ApiTelegramException as e:
            logger.error(f"❌ Erro da API do Telegram: {e}")
            raise HTTPException(status_code=500, detail=f"Erro do Telegram: {str(e)}")
        except Exception as e_tg:
            logger.error(f"❌ Erro ao enviar acesso via Telegram: {e_tg}")
            raise HTTPException(status_code=500, detail=f"Erro ao enviar via Telegram: {str(e_tg)}")
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Erro ao reenviar acesso: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- ROTAS FLOW V2 (HÍBRIDO) ---
@app.get("/api/admin/bots/{bot_id}/flow")
def get_flow(bot_id: int, db: Session = Depends(get_db)):
    f = db.query(BotFlow).filter(BotFlow.bot_id == bot_id).first()
    if not f: return {"msg_boas_vindas": "Olá!", "btn_text_1": "DESBLOQUEAR"}
    return f

@app.post("/api/admin/bots/{bot_id}/flow")
def save_flow(bot_id: int, flow: FlowUpdate, db: Session = Depends(get_db)):
    f = db.query(BotFlow).filter(BotFlow.bot_id == bot_id).first()
    if not f: f = BotFlow(bot_id=bot_id)
    db.add(f)
    f.msg_boas_vindas = flow.msg_boas_vindas
    f.media_url = flow.media_url
    f.btn_text_1 = flow.btn_text_1
    f.autodestruir_1 = flow.autodestruir_1
    f.msg_2_texto = flow.msg_2_texto
    f.msg_2_media = flow.msg_2_media
    f.mostrar_planos_2 = flow.mostrar_planos_2
    db.commit()
    return {"status": "saved"}

@app.get("/api/admin/bots/{bot_id}/flow/steps")
def list_steps(bot_id: int, db: Session = Depends(get_db)):
    return db.query(BotFlowStep).filter(BotFlowStep.bot_id == bot_id).order_by(BotFlowStep.step_order).all()

@app.post("/api/admin/bots/{bot_id}/flow/steps")
def add_step(bot_id: int, p: FlowStepCreate, db: Session = Depends(get_db)):
    ns = BotFlowStep(bot_id=bot_id, step_order=p.step_order, msg_texto=p.msg_texto, msg_media=p.msg_media, btn_texto=p.btn_texto)
    db.add(ns)
    db.commit()
    return {"status": "ok"}

@app.delete("/api/admin/bots/{bot_id}/flow/steps/{sid}")
def del_step(bot_id: int, sid: int, db: Session = Depends(get_db)):
    s = db.query(BotFlowStep).filter(BotFlowStep.id == sid).first()
    if s:
        db.delete(s)
        db.commit()
    return {"status": "deleted"}

# --- NOVA ROTA: DISPARO INDIVIDUAL (VIA HISTÓRICO) ---
class IndividualRemarketingRequest(BaseModel):
    bot_id: int
    user_telegram_id: str
    campaign_history_id: int # ID do histórico para copiar a msg

# ============================================================
# 🔥 CORREÇÃO FINAL: Rota send-individual COM BOTÃO INLINE
# LOCALIZAÇÃO: Linha 2075
# ADICIONA: Botão embutido quando houver oferta
# ============================================================

@app.post("/api/admin/remarketing/send-individual")
def send_individual_remarketing(
    payload: dict,
    db: Session = Depends(get_db)
):
    """
    🔥 [CORRIGIDO] Envia campanha individual com BOTÃO INLINE de oferta
    """
    try:
        bot_id = payload.get("bot_id")
        user_telegram_id = str(payload.get("user_telegram_id"))
        campaign_history_id = payload.get("campaign_history_id")
        
        logger.info(f"📨 Enviando campanha - Bot: {bot_id}, User: {user_telegram_id}, Campaign: {campaign_history_id}")
        
        # Validações
        if not all([bot_id, user_telegram_id, campaign_history_id]):
            raise HTTPException(status_code=400, detail="Campos obrigatórios faltando")
        
        # Buscar bot
        bot = db.query(Bot).filter(Bot.id == bot_id).first()
        if not bot:
            raise HTTPException(status_code=404, detail="Bot não encontrado")
        
        # Buscar campanha
        campaign = db.query(RemarketingCampaign).filter(
            RemarketingCampaign.id == campaign_history_id
        ).first()
        
        if not campaign:
            raise HTTPException(status_code=404, detail="Campanha não encontrada")
        
        # Parsear config
        config = json.loads(campaign.config) if isinstance(campaign.config, str) else campaign.config
        
        mensagem = config.get("mensagem", "")
        media_url = config.get("media_url")
        incluir_oferta = config.get("incluir_oferta", False)
        plano_oferta_id = config.get("plano_oferta_id")
        
        if not mensagem:
            raise HTTPException(status_code=400, detail="Mensagem não encontrada")
        
        # 🔥 [NOVO] Buscar plano e criar botão inline
        markup = None
        if incluir_oferta and plano_oferta_id:
            plano = db.query(PlanoConfig).filter(PlanoConfig.id == int(plano_oferta_id)).first()
            if plano:
                price_mode = config.get("price_mode", "original")
                custom_price = config.get("custom_price", 0)
                
                # Preço a usar
                preco_final = float(custom_price) if price_mode == "custom" else plano.preco_atual
                
                # 🔥 CRIAR BOTÃO INLINE
                markup = types.InlineKeyboardMarkup()
                
                # Texto do botão: "💎 PLANO VIP - R$ 29.90"
                btn_text = f"💎 {plano.nome_exibicao} - R$ {preco_final:.2f}"

                # 🔥 [CORRIGIDO] Callback que funciona no webhook
                if campaign.campaign_id:
                    btn_callback = f"promo_{campaign.campaign_id}"
                    logger.info(f"🎯 Usando callback promo: {btn_callback}")
                else:
                    btn_callback = f"checkout_{plano.id}"
                    logger.info(f"⚠️ Usando callback checkout: {btn_callback}")
                
                # ✅ ADICIONA O BOTÃO AO MARKUP (VOCÊ ESQUECEU ISSO!)
                markup.add(types.InlineKeyboardButton(
                    text=btn_text,
                    callback_data=btn_callback
                ))
        
        # 🔥 ENVIAR usando TELEBOT com BOTÃO
        try:
            bot_instance = telebot.TeleBot(bot.token)
            
            # Enviar mídia ou texto COM O BOTÃO
            if media_url:
                if media_url.lower().endswith(('.mp4', '.avi', '.mov', '.mkv')):
                    # Vídeo com botão
                    bot_instance.send_video(
                        chat_id=user_telegram_id,
                        video=media_url,
                        caption=mensagem,
                        parse_mode='HTML',
                        reply_markup=markup  # 🔥 BOTÃO AQUI
                    )
                else:
                    # Foto com botão
                    bot_instance.send_photo(
                        chat_id=user_telegram_id,
                        photo=media_url,
                        caption=mensagem,
                        parse_mode='HTML',
                        reply_markup=markup  # 🔥 BOTÃO AQUI
                    )
            else:
                # Apenas texto com botão
                bot_instance.send_message(
                    chat_id=user_telegram_id,
                    text=mensagem,
                    parse_mode='HTML',
                    reply_markup=markup  # 🔥 BOTÃO AQUI
                )
            
            logger.info(f"✅ Mensagem com botão enviada para {user_telegram_id}")
            
            return {
                "success": True,
                "message": f"Mensagem enviada com sucesso para {user_telegram_id}"
            }
            
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem: {e}")
            raise HTTPException(status_code=400, detail=f"Erro ao enviar: {str(e)}")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro no envio individual: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# =========================================================
# 📢 LÓGICA DE REMARKETING (ALINHADA COM O FRONTEND + LÓGICA DE CONJUNTOS)
# =========================================================
CAMPAIGN_STATUS = {
    "running": False,
    "sent": 0,
    "total": 0,
    "blocked": 0
}

# =========================================================
# 📨 FUNÇÃO WORKER DE REMARKETING (COMPLETA + HTML)
# =========================================================
def processar_envio_remarketing(bot_id: int, payload: RemarketingRequest):
    # 🔥 [CRÍTICO] Criar nova sessão para background task
    db = SessionLocal()
    
    try:
        global CAMPAIGN_STATUS
        CAMPAIGN_STATUS = {"running": True, "sent": 0, "total": 0, "blocked": 0}
        
        bot_db = db.query(Bot).filter(Bot.id == bot_id).first()
        
        if not bot_db: 
            CAMPAIGN_STATUS["running"] = False
            return

        # --- 1. IDENTIFICAÇÃO DO FILTRO ---
        filtro_limpo = str(payload.target or "").lower().strip()
        
        # Fallback: Se vier tipo_envio (teste manual)
        if payload.tipo_envio:
            filtro_limpo = str(payload.tipo_envio).lower().strip()

        logger.info(f"🚀 INICIANDO DISPARO | Bot: {bot_db.nome} | Filtro: {filtro_limpo}")

        # --- 2. PREPARAÇÃO DA MENSAGEM E OFERTA ---
        uuid_campanha = str(uuid.uuid4())
        data_expiracao = None
        preco_final = 0.0
        plano_db = None

        if payload.incluir_oferta and payload.plano_oferta_id:
            # Busca plano pelo ID ou Key (Lógica Robusta)
            plano_db = db.query(PlanoConfig).filter(
                (PlanoConfig.key_id == str(payload.plano_oferta_id)) | 
                (PlanoConfig.id == int(payload.plano_oferta_id) if str(payload.plano_oferta_id).isdigit() else False)
            ).first()

            if plano_db:
                # Lógica de preço (Custom ou Original)
                if payload.price_mode == 'custom' and payload.custom_price and payload.custom_price > 0:
                    preco_final = payload.custom_price
                else:
                    preco_final = plano_db.preco_atual
                
                # Lógica de expiração
                if payload.expiration_mode != "none" and payload.expiration_value > 0:
                    agora = datetime.utcnow()
                    val = payload.expiration_value
                    if payload.expiration_mode == "minutes": data_expiracao = agora + timedelta(minutes=val)
                    elif payload.expiration_mode == "hours": data_expiracao = agora + timedelta(hours=val)
                    elif payload.expiration_mode == "days": data_expiracao = agora + timedelta(days=val)

        # --- 3. SELEÇÃO DE PÚBLICO (COM SUPORTE COMPLETO A LEADS/FUNIL) ---
        bot_sender = telebot.TeleBot(bot_db.token)
        lista_final_ids = []

        if payload.is_test:
            # Modo Teste
            if payload.specific_user_id:
                lista_final_ids = [str(payload.specific_user_id).strip()]
            else:
                adm = db.query(BotAdmin).filter(BotAdmin.bot_id == bot_id).first()
                if adm: lista_final_ids = [str(adm.telegram_id).strip()]
            logger.info(f"🧪 MODO TESTE: Enviando para {lista_final_ids}")

        else:
            # A) Buscar LEADS (tabela leads) - TOPO do funil
            q_leads = db.query(Lead.user_id).filter(Lead.bot_id == bot_id).distinct()
            ids_leads = {str(r[0]).strip() for r in q_leads.all() if r[0]}
            
            # B) Buscar TODOS os pedidos
            q_todos = db.query(Pedido.telegram_id).filter(Pedido.bot_id == bot_id).distinct()
            ids_pedidos = {str(r[0]).strip() for r in q_todos.all() if r[0]}

            # C) Buscar PAGOS (status_funil='fundo')
            q_pagos = db.query(Pedido.telegram_id).filter(
                Pedido.bot_id == bot_id,
                Pedido.status_funil == 'fundo'
            ).distinct()
            ids_pagantes = {str(r[0]).strip() for r in q_pagos.all() if r[0]}
            
            # D) Buscar MEIO (status_funil='meio')
            q_meio = db.query(Pedido.telegram_id).filter(
                Pedido.bot_id == bot_id,
                Pedido.status_funil == 'meio'
            ).distinct()
            ids_meio = {str(r[0]).strip() for r in q_meio.all() if r[0]}

            # E) Buscar EXPIRADOS (status_funil='expirado')
            q_expirados = db.query(Pedido.telegram_id).filter(
                Pedido.bot_id == bot_id,
                Pedido.status_funil == 'expirado'
            ).distinct()
            ids_expirados = {str(r[0]).strip() for r in q_expirados.all() if r[0]}

            # --- APLICAÇÃO DO FILTRO (SUA LÓGICA COMPLETA) ---
            
            if filtro_limpo == 'topo':
                lista_final_ids = list(ids_leads)
                logger.info(f"🎯 FILTRO TOPO: {len(lista_final_ids)} leads")
            
            elif filtro_limpo == 'meio':
                lista_final_ids = list(ids_meio)
                logger.info(f"🔥 FILTRO MEIO: {len(lista_final_ids)} leads quentes")
            
            elif filtro_limpo == 'fundo':
                lista_final_ids = list(ids_pagantes)
                logger.info(f"✅ FILTRO FUNDO: {len(lista_final_ids)} clientes")
            
            elif filtro_limpo in ['expirado', 'expirados']:
                lista_final_ids = list(ids_expirados)
                logger.info(f"⏰ FILTRO EXPIRADOS: {len(lista_final_ids)} expirados")
            
            elif filtro_limpo in ['pendentes', 'leads', 'nao_pagantes']:
                # PENDENTES: Meio + Expirados
                lista_final_ids = list(ids_meio | ids_expirados)
                logger.info(f"⏳ FILTRO PENDENTES: {len(lista_final_ids)} pendentes")
            
            elif filtro_limpo in ['pagantes', 'ativos']:
                lista_final_ids = list(ids_pagantes)
                logger.info(f"💰 FILTRO PAGANTES: {len(lista_final_ids)} pagantes")
            
            elif filtro_limpo == 'todos':
                lista_final_ids = list(ids_leads | ids_pedidos)
                logger.info(f"👥 FILTRO TODOS: {len(lista_final_ids)} contatos")
            
            else:
                logger.warning(f"⚠️ Filtro desconhecido '{filtro_limpo}'. Assumindo TODOS.")
                lista_final_ids = list(ids_leads | ids_pedidos)

        CAMPAIGN_STATUS["total"] = len(lista_final_ids)

        # --- 4. BOTÃO ---
        markup = None
        if plano_db:
            markup = types.InlineKeyboardMarkup()
            btn_text = f"🔥 {plano_db.nome_exibicao} - R$ {preco_final:.2f}"
            
            if payload.is_test:
                 markup.add(types.InlineKeyboardButton(f"[TESTE] {btn_text}", callback_data=f"checkout_{plano_db.id}"))
            else:
                 # Callback promo aciona o webhook com preço promocional
                 markup.add(types.InlineKeyboardButton(btn_text, callback_data=f"promo_{uuid_campanha}"))

        # --- 5. LOOP DE ENVIO (COM HTML ATUALIZADO) ---
        sent_count = 0
        blocked_count = 0

        for uid in lista_final_ids:
            if not uid or len(uid) < 5: continue
            
            try:
                midia_ok = False
                # Envio com Mídia (HTML)
                if payload.media_url and len(payload.media_url) > 5:
                    try:
                        ext = payload.media_url.lower()
                        if ext.endswith(('.mp4', '.mov', '.avi')):
                            bot_sender.send_video(uid, payload.media_url, caption=payload.mensagem, reply_markup=markup, parse_mode="HTML")
                        else:
                            bot_sender.send_photo(uid, payload.media_url, caption=payload.mensagem, reply_markup=markup, parse_mode="HTML")
                        midia_ok = True
                    except:
                        pass 
                
                # Envio Texto Puro (Se não teve mídia ou falhou) (HTML)
                if not midia_ok:
                    bot_sender.send_message(uid, payload.mensagem, reply_markup=markup, parse_mode="HTML")

                sent_count += 1
                time.sleep(0.04) # Evita flood
                
            except Exception as e:
                err = str(e).lower()
                if "blocked" in err or "kicked" in err or "deactivated" in err or "chat not found" in err:
                    blocked_count += 1

        CAMPAIGN_STATUS["running"] = False
        
        # --- 6. SALVAR NO BANCO (MANTENDO SUA LÓGICA FINAL) ---
        nova_campanha = RemarketingCampaign(
            bot_id=bot_id,
            campaign_id=uuid_campanha,
            target=filtro_limpo,
            config=json.dumps({
                "mensagem": payload.mensagem,
                "media_url": payload.media_url,
                "incluir_oferta": payload.incluir_oferta,
                "plano_oferta_id": payload.plano_oferta_id,
                "price_mode": payload.price_mode,
                "custom_price": payload.custom_price,
                "expiration_mode": payload.expiration_mode,
                "expiration_value": payload.expiration_value
            }),
            total_leads=len(lista_final_ids),
            sent_success=sent_count,
            blocked_count=blocked_count,
            data_envio=datetime.utcnow(),
            plano_id=plano_db.id if plano_db else None,
            promo_price=preco_final if plano_db else None,
            expiration_at=data_expiracao
        )
        db.add(nova_campanha)
        db.commit()
        
        logger.info(f"✅ CAMPANHA FINALIZADA: {sent_count} envios / {blocked_count} bloqueios")
        
    except Exception as e:
        logger.error(f"❌ Erro no processamento: {e}")
        try: db.rollback()
        except: pass
        
    finally:
        db.close()
        logger.info(f"🔒 Sessão do banco fechada")


# --- ROTAS DA API ---

@app.post("/api/admin/remarketing/send")
def enviar_remarketing(payload: RemarketingRequest, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    # Lógica para Teste: Se for teste e não tiver ID, pega o último do banco
    if payload.is_test and not payload.specific_user_id:
        ultimo = db.query(Pedido).filter(Pedido.bot_id == payload.bot_id).order_by(Pedido.id.desc()).first()
        if ultimo:
            payload.specific_user_id = ultimo.telegram_id
        else:
            # Tenta pegar um admin se não tiver clientes
            admin = db.query(BotAdmin).filter(BotAdmin.bot_id == payload.bot_id).first()
            if admin: payload.specific_user_id = admin.telegram_id
            else: raise HTTPException(400, "Nenhum usuário encontrado para teste. Interaja com o bot primeiro (/start).")

    background_tasks.add_task(processar_envio_remarketing, payload.bot_id, payload)
    return {"status": "enviando", "msg": "Campanha iniciada!"}

@app.get("/api/admin/remarketing/status")
def status_remarketing():
    return CAMPAIGN_STATUS

@app.get("/api/admin/remarketing/history/{bot_id}")
def get_remarketing_history(
    bot_id: int, 
    page: int = 1,        # [NOVO] Número da página
    per_page: int = 10,   # [NOVO] Itens por página (padrão 10)
    db: Session = Depends(get_db)
):
    """
    Retorna o histórico de campanhas de remarketing com paginação.
    
    Parâmetros:
    - bot_id: ID do bot
    - page: Número da página (começa em 1)
    - per_page: Registros por página (padrão 10, máximo 50)
    
    Retorna:
    {
        "data": [...],
        "total": 25,
        "page": 1,
        "per_page": 10,
        "total_pages": 3
    }
    """
    
    # Limita per_page a no máximo 50
    per_page = min(per_page, 50)
    
    # Query base
    query = db.query(RemarketingCampaign).filter(
        RemarketingCampaign.bot_id == bot_id
    )
    
    # [NOVO] Conta total de registros
    total_count = query.count()
    
    # [NOVO] Calcula total de páginas
    total_pages = (total_count + per_page - 1) // per_page
    
    # [NOVO] Aplica paginação
    offset = (page - 1) * per_page
    campanhas = query.order_by(RemarketingCampaign.data_envio.desc()).offset(offset).limit(per_page).all()
    
    # Formata resposta
    result = []
    for camp in campanhas:
        result.append({
            "id": camp.id,
            "data_envio": camp.data_envio.isoformat() if camp.data_envio else None,
            "target": camp.target,
            "sent_success": camp.sent_success or 0,
            "blocked_count": camp.blocked_count or 0,
            "total_leads": camp.total_leads or 0,
            "config": camp.config
        })
    
    # [NOVO] Retorna com metadados de paginação
    return {
        "data": result,
        "total": total_count,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages
    }

# ============================================================
# ROTA 2: DELETE HISTÓRICO (NOVA!)
# ============================================================
# COLE ESTA ROTA NOVA logo APÓS a rota de histórico:

@app.delete("/api/admin/remarketing/history/{history_id}")
def delete_remarketing_history(history_id: int, db: Session = Depends(get_db)):
    """
    Deleta uma campanha do histórico.
    """
    campanha = db.query(RemarketingCampaign).filter(
        RemarketingCampaign.id == history_id
    ).first()
    
    if not campanha:
        raise HTTPException(status_code=404, detail="Campanha não encontrada")
    
    db.delete(campanha)
    db.commit()
    
    return {"status": "ok", "message": "Campanha deletada com sucesso"}


# =========================================================
# 📊 ROTA DE DASHBOARD (KPIs REAIS E CUMULATIVOS)
# =========================================================
# =========================================================
# 📊 ROTA DE DASHBOARD V2 (COM FILTRO DE DATA)
# =========================================================
@app.get("/api/admin/dashboard/stats")
def dashboard_stats(
    bot_id: Optional[int] = None, 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None, 
    db: Session = Depends(get_db)
): 
    """
    Dashboard V2: Suporta filtro por período personalizado.
    Padrão: Mês atual.
    """
    try:
        # 1. DEFINIÇÃO DO PERÍODO
        agora = datetime.utcnow()
        
        if start_date and end_date:
            # Se o usuário mandou datas (Filtro Personalizado)
            # O frontend manda em formato ISO (ex: 2023-10-25T14:00:00.000Z)
            # Vamos pegar só a data YYYY-MM-DD
            dt_inicio = datetime.fromisoformat(start_date.replace('Z', '+00:00')).replace(hour=0, minute=0, second=0, tzinfo=None)
            dt_fim = datetime.fromisoformat(end_date.replace('Z', '+00:00')).replace(hour=23, minute=59, second=59, tzinfo=None)
        else:
            # Padrão: Mês Atual (Do dia 1 até agora)
            dt_inicio = datetime(agora.year, agora.month, 1)
            dt_fim = agora

        # Data de hoje (para comparação de "Vendas Hoje")
        hoje_inicio = datetime(agora.year, agora.month, agora.day)

        # 2. QUERIES BASE
        query_leads = db.query(Lead)
        query_pedidos = db.query(Pedido)

        if bot_id:
            query_leads = query_leads.filter(Lead.bot_id == bot_id)
            query_pedidos = query_pedidos.filter(Pedido.bot_id == bot_id)

        # --- KPIS FILTRADOS PELO PERÍODO SELECIONADO ---
        
        # Leads no período
        leads_periodo = query_leads.filter(
            Lead.created_at >= dt_inicio,
            Lead.created_at <= dt_fim
        ).count()

        # Novos Leads Hoje (Sempre fixo "Hoje" independentemente do filtro, para referência)
        leads_hoje = query_leads.filter(Lead.created_at >= hoje_inicio).count()
        
        # Status de pagamento confirmado
        status_pagos = ['paid', 'active', 'approved', 'completed', 'succeeded']
        
        # Vendas no período selecionado
        vendas_periodo = query_pedidos.filter(
            Pedido.status.in_(status_pagos),
            Pedido.created_at >= dt_inicio,
            Pedido.created_at <= dt_fim
        )
        
        # Total Faturamento (No período)
        total_revenue = db.query(func.sum(Pedido.valor)).filter(
            Pedido.status.in_(status_pagos),
            Pedido.created_at >= dt_inicio,
            Pedido.created_at <= dt_fim
        )
        if bot_id: total_revenue = total_revenue.filter(Pedido.bot_id == bot_id)
        valor_total_revenue = total_revenue.scalar() or 0.0

        # Assinantes (Quantidade de vendas no período)
        total_transactions = vendas_periodo.count()
        
        # Assinantes Ativos Totais (Base geral, independente do filtro de data)
        # É legal mostrar a base total mesmo filtrando datas passadas
        total_active_users = db.query(Pedido).filter(Pedido.status.in_(status_pagos))
        if bot_id: total_active_users = total_active_users.filter(Pedido.bot_id == bot_id)
        count_active_users = total_active_users.count()

        # Ticket Médio (No período)
        ticket_medio = (valor_total_revenue / total_transactions) if total_transactions > 0 else 0.0

        # Vendas Hoje (Sempre fixo "Hoje")
        sales_today_query = db.query(func.sum(Pedido.valor)).filter(
            Pedido.status.in_(status_pagos),
            Pedido.created_at >= hoje_inicio
        )
        if bot_id: sales_today_query = sales_today_query.filter(Pedido.bot_id == bot_id)
        valor_sales_today = sales_today_query.scalar() or 0.0

        # Taxa de Conversão (No período)
        universo_total = leads_periodo + total_transactions
        conversao = (total_transactions / universo_total * 100) if universo_total > 0 else 0.0

        # --- DADOS DO GRÁFICO (DIÁRIO DENTRO DO PERÍODO) ---
        chart_data = []
        
        # Calcula quantos dias tem no intervalo
        delta = (dt_fim - dt_inicio).days
        
        # Se o intervalo for muito grande (> 60 dias), agrupar por mês ou limitar (Opcional)
        # Por enquanto, vamos manter diário, mas limitando o loop para não explodir
        dias_para_grafico = min(delta + 1, 366) # Limite de 1 ano para visualização diária
        
        for i in range(dias_para_grafico):
            data_loop = dt_inicio + timedelta(days=i)
            data_loop_fim = data_loop + timedelta(days=1) # Fim do dia atual do loop
            
            # Soma vendas daquele dia
            soma_dia = db.query(func.sum(Pedido.valor)).filter(
                Pedido.status.in_(status_pagos),
                Pedido.created_at >= data_loop,
                Pedido.created_at < data_loop_fim
            )
            if bot_id: soma_dia = soma_dia.filter(Pedido.bot_id == bot_id)
            
            val = soma_dia.scalar() or 0.0
            
            chart_data.append({
                "name": data_loop.strftime("%d/%m"), 
                "value": val
            })

        return {
            "total_revenue": valor_total_revenue,
            "active_users": count_active_users, # Base Total
            "sales_today": valor_sales_today,
            "leads_mes": leads_periodo,         # Agora representa Leads do FILTRO
            "leads_hoje": leads_hoje,
            "ticket_medio": ticket_medio,
            "total_transacoes": total_transactions,
            "reembolsos": 0.0,
            "taxa_conversao": round(conversao, 2),
            "chart_data": chart_data
        }

    except Exception as e:
        logger.error(f"❌ Erro no dashboard stats: {str(e)}")
        return {
            "total_revenue": 0.0, "active_users": 0, "sales_today": 0.0,
            "leads_mes": 0, "leads_hoje": 0, "ticket_medio": 0.0,
            "total_transacoes": 0, "reembolsos": 0.0, "taxa_conversao": 0.0,
            "chart_data": []
        }
# =========================================================
# 💸 WEBHOOK DE PAGAMENTO (BLINDADO E TAGARELA)
# =========================================================
@app.post("/api/webhook")
async def webhook(req: Request, bg_tasks: BackgroundTasks):
    try:
        raw = await req.body()
        try: 
            payload = json.loads(raw)
        except: 
            # Fallback para formato x-www-form-urlencoded
            payload = {k: v[0] for k,v in parse_qs(raw.decode()).items()}
        
        # Log para debug (opcional, pode remover em produção)
        # logger.info(f"Webhook recebido: {payload}")

        # Se for pagamento APROVADO (Vários status possíveis de gateways)
        if str(payload.get('status')).upper() in ['PAID', 'APPROVED', 'COMPLETED', 'SUCCEEDED']:
            db = SessionLocal()
            tx = str(payload.get('id')).lower() # ID da transação
            
            # Busca o pedido pelo ID da transação
            p = db.query(Pedido).filter(Pedido.transaction_id == tx).first()
            
            # Se achou o pedido e ele ainda não estava pago
            if p and p.status != 'paid':
                p.status = 'paid'
                db.commit() # Salva o status pago
                
                # --- 🔔 NOTIFICAÇÃO AO ADMIN (NOVO) ---
                try:
                    bot_db = db.query(Bot).filter(Bot.id == p.bot_id).first()
                    
                    # Verifica se o bot tem um Admin configurado para receber o aviso
                    if bot_db and bot_db.admin_principal_id:
                        msg_venda = (
                            f"💰 *VENDA APROVADA!*\n\n"
                            f"👤 Cliente: {p.first_name}\n"
                            f"💎 Plano: {p.plano_nome}\n"
                            f"💵 Valor: R$ {p.valor:.2f}\n"
                            f"📅 Data: {datetime.now().strftime('%d/%m %H:%M')}"
                        )
                        # Chama a função auxiliar de notificação
                        notificar_admin_principal(bot_db, msg_venda) 
                except Exception as e_notify:
                    logger.error(f"Erro ao notificar admin: {e_notify}")
                # --------------------------------------

                # --- ENVIO DO LINK DE ACESSO AO CLIENTE ---
                if not p.mensagem_enviada:
                    try:
                        bot_data = db.query(Bot).filter(Bot.id == p.bot_id).first()
                        tb = telebot.TeleBot(bot_data.token)
                        
                        # Tenta converter o ID do canal VIP com segurança
                        try: canal_vip_id = int(str(bot_data.id_canal_vip).strip())
                        except: canal_vip_id = bot_data.id_canal_vip

                        # Tenta desbanir o usuário antes (garantia caso ele tenha sido expulso antes)
                        try: tb.unban_chat_member(canal_vip_id, int(p.telegram_id))
                        except: pass

                        # Gera Link Único (Válido para 1 pessoa)
                        convite = tb.create_chat_invite_link(
                            chat_id=canal_vip_id, 
                            member_limit=1, 
                            name=f"Venda {p.first_name}"
                        )
                        link_acesso = convite.invite_link

                        msg_sucesso = f"""
✅ <b>Pagamento Confirmado!</b>

Seu acesso ao <b>{bot_data.nome}</b> foi liberado.
Toque no link abaixo para entrar no Canal VIP:

👉 {link_acesso}

⚠️ <i>Este link é único e válido apenas para você.</i>
"""
                        # Envia a mensagem com o link para o usuário
                        tb.send_message(int(p.telegram_id), msg_sucesso, parse_mode="HTML")
                        
                        # Marca que a mensagem foi enviada para não enviar duplicado
                        p.mensagem_enviada = True
                        db.commit()
                        logger.info(f"🏆 Link enviado para {p.first_name}")

                    except Exception as e_telegram:
                        logger.error(f"❌ ERRO TELEGRAM: {e_telegram}")
                        # Fallback: Avisa o cliente que deu erro no envio do link, mas confirma o pagamento
                        try:
                            tb.send_message(int(p.telegram_id), "✅ Pagamento recebido! \n\n⚠️ Houve um erro ao gerar seu link automático. Um administrador entrará em contato em breve.")
                        except: pass

            db.close()
        
        # Retorna 200 OK para o Gateway de Pagamento parar de mandar o Webhook
        return {"status": "received"}

    except Exception as e:
        logger.error(f"❌ ERRO CRÍTICO NO WEBHOOK: {e}")
        # Mesmo com erro, retornamos 200 ou estrutura json para não travar o gateway (opcional, depende da estratégia)
        return {"status": "error"}

# ============================================================
# TRECHO 3: FUNÇÃO "enviar_passo_automatico"
# ============================================================

# ============================================================
# TRECHO 3: FUNÇÃO "enviar_passo_automatico" (CORRIGIDA + HTML)
# ============================================================

def enviar_passo_automatico(bot_temp, chat_id, passo, bot_db, db):
    """
    Envia um passo automaticamente após o delay (COM HTML).
    Similar à lógica do next_step_, mas sem callback do usuário.
    """
    logger.info(f"✅ [BOT {bot_db.id}] Enviando passo {passo.step_order} automaticamente: {passo.msg_texto[:30]}...")
    
    # 1. Verifica se existe passo seguinte (CRÍTICO: Fazer isso ANTES de tudo)
    passo_seguinte = db.query(BotFlowStep).filter(
        BotFlowStep.bot_id == bot_db.id, 
        BotFlowStep.step_order == passo.step_order + 1
    ).first()
    
    # 2. Define o callback do botão (Baseado na existência do próximo)
    if passo_seguinte:
        next_callback = f"next_step_{passo.step_order}"
    else:
        next_callback = "go_checkout"
    
    # 3. Cria botão (se configurado para mostrar)
    markup_step = types.InlineKeyboardMarkup()
    if passo.mostrar_botao:
        markup_step.add(types.InlineKeyboardButton(
            text=passo.btn_texto, 
            callback_data=next_callback
        ))
    
    # 4. Envia a mensagem e SALVA o message_id (Com HTML)
    sent_msg = None
    try:
        if passo.msg_media:
            try:
                if passo.msg_media.lower().endswith(('.mp4', '.mov')):
                    sent_msg = bot_temp.send_video(
                        chat_id, 
                        passo.msg_media, 
                        caption=passo.msg_texto, 
                        reply_markup=markup_step if passo.mostrar_botao else None,
                        parse_mode="HTML" # 🔥 Adicionado HTML
                    )
                else:
                    sent_msg = bot_temp.send_photo(
                        chat_id, 
                        passo.msg_media, 
                        caption=passo.msg_texto, 
                        reply_markup=markup_step if passo.mostrar_botao else None,
                        parse_mode="HTML" # 🔥 Adicionado HTML
                    )
            except Exception as e_media:
                logger.error(f"Erro ao enviar mídia no passo automático: {e_media}")
                # Fallback para texto se a mídia falhar
                sent_msg = bot_temp.send_message(
                    chat_id, 
                    passo.msg_texto, 
                    reply_markup=markup_step if passo.mostrar_botao else None,
                    parse_mode="HTML" # 🔥 Adicionado HTML
                )
        else:
            sent_msg = bot_temp.send_message(
                chat_id, 
                passo.msg_texto, 
                reply_markup=markup_step if passo.mostrar_botao else None,
                parse_mode="HTML" # 🔥 Adicionado HTML
            )
        
        # 5. Lógica Automática (Recursividade e Delay)
        # Se NÃO tem botão E tem delay E tem próximo passo
        if not passo.mostrar_botao and passo.delay_seconds > 0 and passo_seguinte:
            logger.info(f"⏰ [BOT {bot_db.id}] Aguardando {passo.delay_seconds}s antes do próximo...")
            time.sleep(passo.delay_seconds)
            
            # Auto-destruir antes de enviar a próxima
            if passo.autodestruir and sent_msg:
                try:
                    bot_temp.delete_message(chat_id, sent_msg.message_id)
                    logger.info(f"💣 [BOT {bot_db.id}] Mensagem do passo {passo.step_order} auto-destruída (automático)")
                except:
                    pass
            
            # Chama o próximo passo (Recursivo)
            enviar_passo_automatico(bot_temp, chat_id, passo_seguinte, bot_db, db)
            
        # Se NÃO tem botão E NÃO tem próximo passo (Fim da Linha)
        elif not passo.mostrar_botao and not passo_seguinte:
            # Acabaram os passos, vai pro checkout (Oferta Final)
            # Se tiver delay no último passo antes da oferta, espera também
            if passo.delay_seconds > 0:
                 time.sleep(passo.delay_seconds)
                 
            enviar_oferta_final(bot_temp, chat_id, bot_db.fluxo, bot_db.id, db)
            
    except Exception as e:
        logger.error(f"❌ [BOT {bot_db.id}] Erro crítico ao enviar passo automático: {e}")

# =========================================================
# 📤 FUNÇÃO AUXILIAR: ENVIAR OFERTA FINAL
# =========================================================
def enviar_oferta_final(tb, cid, fluxo, bot_id, db):
    """Envia a oferta final (Planos)"""
    mk = types.InlineKeyboardMarkup()
    planos = db.query(PlanoConfig).filter(PlanoConfig.bot_id == bot_id).all()
    
    if fluxo and fluxo.mostrar_planos_2:
        for p in planos:
            mk.add(types.InlineKeyboardButton(
                f"💎 {p.nome_exibicao} - R$ {p.preco_atual:.2f}", 
                callback_data=f"checkout_{p.id}"
            ))
    
    txt = fluxo.msg_2_texto if (fluxo and fluxo.msg_2_texto) else "Escolha seu plano:"
    med = fluxo.msg_2_media if fluxo else None
    
    try:
        if med:
            if med.endswith(('.mp4','.mov')): 
                tb.send_video(cid, med, caption=txt, reply_markup=mk)
            else: 
                tb.send_photo(cid, med, caption=txt, reply_markup=mk)
        else:
            tb.send_message(cid, txt, reply_markup=mk)
    except:
        tb.send_message(cid, txt, reply_markup=mk)
# =========================================================
# 🏆 ROTA DE PERFIL & CONQUISTAS (PERFIL GLOBAL)
# =========================================================
@app.get("/api/admin/profile")
def get_profile_stats(db: Session = Depends(get_db)):
    """
    Retorna dados do perfil do administrador + Estatísticas GLOBAIS de todos os bots.
    Calcula o progresso para a próxima placa de faturamento.
    """
    try:
        # 1. Busca Configurações de Perfil (SystemConfig)
        conf_name = db.query(SystemConfig).filter(SystemConfig.key == "admin_name").first()
        conf_avatar = db.query(SystemConfig).filter(SystemConfig.key == "admin_avatar").first()
        
        admin_name = conf_name.value if conf_name else "Administrador"
        admin_avatar = conf_avatar.value if conf_avatar else ""

        # 2. Estatísticas GLOBAIS (Soma de tudo)
        
        # A) Total de Bots Ativos
        total_bots = db.query(Bot).count()
        
        # B) Total de Membros (Leads Únicos + Pedidos Únicos de TODOS os bots)
        q_leads = db.query(Lead.user_id).all()
        q_pedidos = db.query(Pedido.telegram_id).all()
        
        # Usa SET para contar usuários únicos globais
        unique_members = set()
        for r in q_leads:
            if r[0]: unique_members.add(str(r[0]))
        for r in q_pedidos:
            if r[0]: unique_members.add(str(r[0]))
            
        total_members = len(unique_members)
        
        # C) Faturamento Total (Apenas Pagos/Aprovados)
        status_ok = ['paid', 'approved', 'active', 'completed', 'succeeded']
        revenue_query = db.query(func.sum(Pedido.valor)).filter(Pedido.status.in_(status_ok))
        total_revenue = revenue_query.scalar() or 0.0
        
        # D) Total de Vendas (Quantidade)
        total_sales = db.query(Pedido).filter(Pedido.status.in_(status_ok)).count()
        
        # 3. Lógica de Gamificação (Placas)
        # Níveis: 100k (Prodígio), 500k (Empreendedor), 1M (Milionário), 10M (Magnata)
        levels = [
            {"name": "Prodígio", "target": 100000, "slug": "prodigio"},
            {"name": "Empreendedor", "target": 500000, "slug": "empreendedor"},
            {"name": "Milionário", "target": 1000000, "slug": "milionario"},
            {"name": "Magnata", "target": 10000000, "slug": "magnata"}
        ]
        
        current_level = None
        next_level = levels[0]
        
        for lvl in levels:
            if total_revenue >= lvl["target"]:
                current_level = lvl
            else:
                next_level = lvl
                break
        
        # Se passou do último nível
        if total_revenue >= levels[-1]["target"]:
            next_level = None 
            
        # Cálculo da porcentagem para a barra de progresso
        if next_level:
            # Ex: Faturou 20k, Meta 100k -> 20%
            progress_pct = (total_revenue / next_level["target"]) * 100
            progress_pct = min(progress_pct, 100)
        else:
            progress_pct = 100 # Zerou o jogo
            
        return {
            "profile": {
                "name": admin_name,
                "avatar_url": admin_avatar
            },
            "stats": {
                "total_bots": total_bots,
                "total_members": total_members,
                "total_revenue": total_revenue,
                "total_sales": total_sales
            },
            "gamification": {
                "current_level": current_level,
                "next_level": next_level,
                "progress_percentage": round(progress_pct, 2)
            }
        }

    except Exception as e:
        logger.error(f"Erro ao buscar perfil: {e}")
        return {"error": str(e)}

@app.post("/api/admin/profile")
def update_profile(data: ProfileUpdate, db: Session = Depends(get_db)):
    """
    Atualiza Nome e Foto do Administrador
    """
    try:
        # Atualiza ou Cria Nome
        conf_name = db.query(SystemConfig).filter(SystemConfig.key == "admin_name").first()
        if not conf_name:
            conf_name = SystemConfig(key="admin_name")
            db.add(conf_name)
        conf_name.value = data.name
        
        # Atualiza ou Cria Avatar
        conf_avatar = db.query(SystemConfig).filter(SystemConfig.key == "admin_avatar").first()
        if not conf_avatar:
            conf_avatar = SystemConfig(key="admin_avatar")
            db.add(conf_avatar)
        conf_avatar.value = data.avatar_url or ""
        
        db.commit()
        return {"status": "success", "msg": "Perfil atualizado!"}
        
    except Exception as e:
        logger.error(f"Erro ao atualizar perfil: {e}")
        raise HTTPException(status_code=500, detail="Erro ao salvar perfil")

@app.get("/")
def home():

    return {"status": "Zenyx SaaS Online - Banco Atualizado"}
@app.get("/admin/clean-leads-to-pedidos")
def limpar_leads_que_viraram_pedidos(db: Session = Depends(get_db)):
    """
    Remove da tabela LEADS os usuários que já geraram PEDIDOS.
    Evita duplicação entre TOPO (leads) e TODOS (pedidos).
    """
    try:
        total_removidos = 0
        bots = db.query(Bot).all()
        
        for bot in bots:
            # Buscar todos os telegram_ids que existem em PEDIDOS
            pedidos_ids = db.query(Pedido.telegram_id).filter(
                Pedido.bot_id == bot.id
            ).distinct().all()
            
            pedidos_ids = [str(pid[0]) for pid in pedidos_ids if pid[0]]
            
            # Deletar LEADS que têm user_id igual a algum telegram_id dos pedidos
            for telegram_id in pedidos_ids:
                leads_para_deletar = db.query(Lead).filter(
                    Lead.bot_id == bot.id,
                    Lead.user_id == telegram_id
                ).all()
                
                for lead in leads_para_deletar:
                    db.delete(lead)
                    total_removidos += 1
        
        db.commit()
        
        return {
            "status": "ok",
            "leads_removidos": total_removidos,
            "mensagem": f"Removidos {total_removidos} leads que viraram pedidos"
        }
    
    except Exception as e:
        db.rollback()
        logger.error(f"Erro: {e}")
        return {"status": "error", "mensagem": str(e)}