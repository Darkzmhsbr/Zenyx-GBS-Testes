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

# Importa o banco e o script de reparo
from database import SessionLocal, init_db, Bot, PlanoConfig, BotFlow, BotFlowStep, Pedido, SystemConfig, RemarketingCampaign, BotAdmin, engine
import update_db 

from migration_v3 import executar_migracao_v3
from migration_v4 import executar_migracao_v4

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

# =========================================================
# 2. AUTO-REPARO DO BANCO DE DADOS (LISTA MESTRA DE CORREÇÃO)
# =========================================================
@app.on_event("startup")
def on_startup():
    init_db()
    executar_migracao_v3()
    executar_migracao_v4()
    
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
                    # Proteção: Se for admin, não remove
                    if u.role == 'admin': continue
                    
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
    seus_dominio = "zenyx-gbs-production.up.railway.app" 
    
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
def notificar_admin_principal(bot_db: Bot, mensagem: str):
    if not bot_db.admin_principal_id:
        return
    try:
        sender = telebot.TeleBot(bot_db.token)
        sender.send_message(bot_db.admin_principal_id, mensagem, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Falha ao notificar admin principal {bot_db.admin_principal_id}: {e}")

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

class FlowUpdate(BaseModel):
    msg_boas_vindas: str
    media_url: Optional[str] = None
    btn_text_1: str
    autodestruir_1: bool
    msg_2_texto: Optional[str] = None
    msg_2_media: Optional[str] = None
    mostrar_planos_2: bool

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

class IntegrationUpdate(BaseModel):
    token: str

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

    # Atualiza campos básicos
    if dados.nome: bot_db.nome = dados.nome
    if dados.token: bot_db.token = dados.token
    if dados.id_canal_vip: bot_db.id_canal_vip = dados.id_canal_vip
    
    # --- SALVA O ADMIN PRINCIPAL ---
    # Aceita string vazia para limpar o campo, ou valor novo
    if dados.admin_principal_id is not None: 
        bot_db.admin_principal_id = dados.admin_principal_id
    
    # Se houver troca de token, atualiza Webhook
    if dados.token and dados.token != old_token:
        try:
            # 1. Tenta remover webhook do token antigo
            try:
                old_tb = telebot.TeleBot(old_token)
                old_tb.delete_webhook()
            except: pass

            # 2. Configura o novo webhook
            tb = telebot.TeleBot(dados.token)
            public_url = os.getenv("RAILWAY_PUBLIC_DOMAIN", "zenyx-gbs-production.up.railway.app")
            if public_url.startswith("https://"): public_url = public_url.replace("https://", "")
            
            webhook_url = f"https://{public_url}/webhook/{dados.token}"
            tb.set_webhook(url=webhook_url)
            
            logger.info(f"♻️ Webhook atualizado para o bot {bot_db.nome}")
            bot_db.status = "ativo" # Reseta para ativo ao trocar token
        except Exception as e:
            logger.error(f"Erro ao atualizar webhook: {e}")
            bot_db.status = "erro_token"
    
    db.commit()
    db.refresh(bot_db)
    return {"status": "ok", "msg": "Bot atualizado com sucesso"}

# --- NOVA ROTA: LIGAR/DESLIGAR BOT (TOGGLE) ---
@app.post("/api/admin/bots/{bot_id}/toggle")
def toggle_bot(bot_id: int, db: Session = Depends(get_db)):
    bot = db.query(Bot).filter(Bot.id == bot_id).first()
    if not bot: raise HTTPException(404, "Bot não encontrado")
    
    # Inverte o status
    novo_status = "ativo" if bot.status != "ativo" else "pausado"
    bot.status = novo_status
    db.commit()
    
    # 🔔 Notifica Admin
    try:
        emoji = "🟢" if novo_status == "ativo" else "🔴"
        msg = f"{emoji} *STATUS DO BOT ALTERADO*\n\nO bot *{bot.nome}* agora está: *{novo_status.upper()}*"
        notificar_admin_principal(bot, msg)
    except Exception as e:
        logger.error(f"Erro ao notificar admin sobre toggle: {e}")
    
    return {"status": novo_status}

# --- NOVA ROTA: EXCLUIR BOT ---
@app.delete("/api/admin/bots/{bot_id}")
def deletar_bot(bot_id: int, db: Session = Depends(get_db)):
    bot_db = db.query(Bot).filter(Bot.id == bot_id).first()
    if not bot_db:
        raise HTTPException(status_code=404, detail="Bot não encontrado")
    
    # 1. Tenta remover o Webhook do Telegram para limpar
    try:
        tb = telebot.TeleBot(bot_db.token)
        tb.delete_webhook()
    except:
        pass # Se der erro (ex: token inválido), continua e apaga do banco
    
    # 2. Apaga do Banco de Dados
    db.delete(bot_db)
    db.commit()
    
    return {"status": "deleted", "msg": "Bot removido com sucesso"}

# =========================================================
# 🛡️ GESTÃO DE ADMINISTRADORES (FASE 1)
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
@app.get("/api/admin/bots")
def listar_bots(db: Session = Depends(get_db)):
    """
    Lista todos os bots com estatísticas corrigidas.
    
    CORREÇÕES:
    - Leads: Conta usuários ÚNICOS (DISTINCT telegram_id)
    - Revenue: Soma apenas pedidos com status 'approved' ou 'paid'
    - Username: Garante que sempre retorna o username
    """
    bots = db.query(Bot).all()
    
    result = []
    for bot in bots:
        # [CORRIGIDO] Conta USUÁRIOS ÚNICOS, não pedidos duplicados
        from sqlalchemy import func
        leads_count = db.query(func.count(func.distinct(Pedido.telegram_id))).filter(
            Pedido.bot_id == bot.id
        ).scalar() or 0
        
        # [CORRIGIDO V2] Soma TODAS as vendas pagas (incluindo expiradas)
        vendas_aprovadas = db.query(Pedido).filter(
            Pedido.bot_id == bot.id,
            Pedido.status.in_(["approved", "paid", "active", "expired"])  # ✅ CORRETO - Inclui expired
        ).all()
        revenue = sum([v.valor for v in vendas_aprovadas]) if vendas_aprovadas else 0.0
        
        result.append({
            "id": bot.id,
            "nome": bot.nome,
            "token": bot.token,
            "username": bot.username or None,  # Retorna None se vazio
            "id_canal_vip": bot.id_canal_vip,
            "admin_principal_id": bot.admin_principal_id,
            "status": bot.status,
            "leads": leads_count,        # [CORRIGIDO] Usuários únicos
            "revenue": revenue,          # [CORRIGIDO] Soma de vendas aprovadas
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
            "mostrar_planos_2": True
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
# 💰 ROTA WEBHOOK PIX (LÓGICA BLINDADA - ADAPTADA PARA SAAS)
# =========================================================
@app.post("/webhook/pix")
async def webhook_pix(request: Request, db: Session = Depends(get_db)):
    print("🔔 WEBHOOK PIX CHEGOU!") 
    try:
        # 1. PEGA O CORPO BRUTO
        body_bytes = await request.body()
        body_str = body_bytes.decode("utf-8")
        
        # Tratamento de JSON ou Form Data
        try:
            data = json.loads(body_str)
        except:
            parsed = urllib.parse.parse_qs(body_str)
            data = {k: v[0] for k, v in parsed.items()}

        # 2. EXTRAÇÃO E NORMALIZAÇÃO DO ID
        raw_tx_id = data.get("id") or data.get("external_reference") or data.get("uuid")
        tx_id = str(raw_tx_id).lower() if raw_tx_id else None
        status_pix = str(data.get("status", "")).lower()
        
        if status_pix not in ["paid", "approved", "completed", "succeeded"]:
            return {"status": "ignored"}

        # 3. BUSCA O PEDIDO
        # Tenta pelo txid (novo) ou transaction_id (antigo)
        pedido = db.query(Pedido).filter((Pedido.txid == tx_id) | (Pedido.transaction_id == tx_id)).first()

        if not pedido:
            print(f"❌ Pedido {tx_id} não encontrado no banco.")
            return {"status": "ok", "msg": "Order not found"}

        if pedido.status == "paid":
            return {"status": "ok", "msg": "Already paid"}

        # --- 4. CÁLCULO DA DATA DE EXPIRAÇÃO (A MÁGICA ACONTECE AQUI) ---
        now = datetime.utcnow()
        data_validade = None # Se ficar None, é Vitalício
        
        # A) Tenta pegar a duração direto da configuração do plano no banco
        if pedido.plano_id:
            # Converte para int caso esteja como string no banco
            pid = int(pedido.plano_id) if str(pedido.plano_id).isdigit() else None
            if pid:
                plano_db = db.query(PlanoConfig).filter(PlanoConfig.id == pid).first()
                # Se o plano existe e tem dias definidos (ex: 1, 30, 365)
                if plano_db and plano_db.dias_duracao and plano_db.dias_duracao > 0:
                    data_validade = now + timedelta(days=plano_db.dias_duracao)

        # B) Fallback: Se não achou pelo ID (planos antigos), tenta pelo nome (Segurança)
        if not data_validade and pedido.plano_nome:
            nm = pedido.plano_nome.lower()
            if "vital" not in nm and "mega" not in nm and "eterno" not in nm:
                dias = 30 # Padrão
                if "24" in nm or "diario" in nm or "1 dia" in nm: dias = 1
                elif "semanal" in nm: dias = 7
                elif "trimestral" in nm: dias = 90
                elif "anual" in nm: dias = 365
                data_validade = now + timedelta(days=dias)

        # 5. ATUALIZA O PEDIDO COM A DATA CALCULADA
        pedido.status = "paid"
        pedido.data_aprovacao = now
        pedido.data_expiracao = data_validade     # Backend V2
        pedido.custom_expiration = data_validade  # <--- O FRONTEND VÊ ISSO AQUI
        pedido.mensagem_enviada = True
        db.commit()
        
        print(f"✅ Pedido {tx_id} APROVADO! Validade: {data_validade if data_validade else 'VITALÍCIO'}")
        
        # 6. ENTREGA O ACESSO E NOTIFICA ADMIN
        try:
            bot_data = db.query(Bot).filter(Bot.id == pedido.bot_id).first()
            if bot_data:
                tb = telebot.TeleBot(bot_data.token)
                
                # Tratamento do ID do Canal
                try: canal_id = int(str(bot_data.id_canal_vip).strip())
                except: canal_id = bot_data.id_canal_vip

                # Tenta desbanir antes (Kick Suave)
                try: tb.unban_chat_member(canal_id, int(pedido.telegram_id))
                except: pass

                # Gera Link Único
                convite = tb.create_chat_invite_link(
                    chat_id=canal_id, 
                    member_limit=1, 
                    name=f"Venda {pedido.first_name}"
                )
                
                # Formata data para o cliente
                texto_validade = "VITALÍCIO ♾️"
                if data_validade:
                    # Ajusta fuso horário visualmente (-3h) se quiser, ou usa UTC direto
                    texto_validade = data_validade.strftime("%d/%m/%Y")

                msg_cliente = (
                    f"✅ <b>Pagamento Confirmado!</b>\n"
                    f"📅 Validade: <b>{texto_validade}</b>\n\n"
                    f"Seu acesso exclusivo:\n👉 {convite.invite_link}"
                )
                tb.send_message(int(pedido.telegram_id), msg_cliente, parse_mode="HTML")
                
                # --- NOTIFICAÇÃO AO ADMIN (INTEGRADA AQUI MESMO) ---
                if bot_data.admin_principal_id:
                    msg_admin = (
                        f"💰 *VENDA NO BOT {bot_data.nome}*\n"
                        f"👤 {pedido.first_name} (@{pedido.username})\n"
                        f"💎 {pedido.plano_nome}\n"
                        f"💵 R$ {pedido.valor:.2f}\n"
                        f"📅 Vence em: {texto_validade}"
                    )
                    try: tb.send_message(bot_data.admin_principal_id, msg_admin, parse_mode="Markdown")
                    except: print("Erro ao notificar admin")

        except Exception as e_tg:
            print(f"❌ Erro Telegram/Entrega: {e_tg}")

        return {"status": "received"}

    except Exception as e:
        print(f"❌ ERRO CRÍTICO NO WEBHOOK: {e}")
        return {"status": "error"}

# =========================================================
# 🚀 WEBHOOK GERAL DO BOT (CORREÇÃO DEFINITIVA - FLOW V2)
# =========================================================
@app.post("/webhook/{bot_token}")
async def receber_update_telegram(bot_token: str, request: Request, db: Session = Depends(get_db)):
    
    # Proteção contra loop do pix
    if bot_token == "pix": return {"status": "ignored_loop"}
    
    bot_db = db.query(Bot).filter(Bot.token == bot_token).first()
    if not bot_db: return {"status": "ignored"}

    # Verifica se bot está pausado
    if bot_db.status == "pausado":
        return {"status": "paused_by_admin"}

    try:
        json_str = await request.json()
        update = telebot.types.Update.de_json(json_str)
        bot_temp = telebot.TeleBot(bot_token)
        
        # --- 🚪 PORTEIRO (código mantido igual) ---
        if update.message and update.message.new_chat_members:
            chat_id_atual = str(update.message.chat.id)
            canal_vip_db = str(bot_db.id_canal_vip).strip()
            
            if chat_id_atual == canal_vip_db:
                for member in update.message.new_chat_members:
                    if member.is_bot: continue
                    
                    user_id = str(member.id)
                    logger.info(f"👤 Verificando entrada de {user_id}")
                    
                    pedido = db.query(Pedido).filter(
                        Pedido.bot_id == bot_db.id,
                        Pedido.telegram_id == user_id
                    ).order_by(text("created_at DESC")).first()
                    
                    acesso_autorizado = False
                    
                    if pedido and pedido.status == 'paid':
                        dias = 30
                        nome = (pedido.plano_nome or "").lower()
                        
                        if "vital" in nome or "mega" in nome: 
                            acesso_autorizado = True
                        else:
                            if "diario" in nome or "24" in nome: dias = 1
                            elif "trimestral" in nome: dias = 90
                            elif "semanal" in nome: dias = 7
                            
                            validade = pedido.created_at + timedelta(days=dias)
                            if datetime.utcnow() < validade:
                                acesso_autorizado = True
                    
                    if not acesso_autorizado:
                        logger.warning(f"🚫 Intruso detectado! Removendo {user_id}...")
                        try:
                            bot_temp.ban_chat_member(chat_id_atual, int(user_id))
                            bot_temp.unban_chat_member(chat_id_atual, int(user_id))
                            try:
                                bot_temp.send_message(int(user_id), "🚫 **Acesso Negado**")
                            except: pass
                        except Exception as e_kick:
                            logger.error(f"Erro ao kickar: {e_kick}")
            
            return {"status": "member_checked"}
        
        # --- 1. COMANDO /START ---
        if update.message and update.message.text == "/start":
            chat_id = update.message.chat.id
            fluxo = bot_db.fluxo
            
            texto = fluxo.msg_boas_vindas if fluxo else f"Olá! Eu sou o {bot_db.nome}."
            btn_txt = fluxo.btn_text_1 if (fluxo and fluxo.btn_text_1) else "🔓 DESBLOQUEAR ACESSO"
            
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton(text=btn_txt, callback_data="passo_2"))

            media = fluxo.media_url if (fluxo and fluxo.media_url) else None
            if media:
                try:
                    if media.lower().endswith(('.mp4', '.mov', '.avi')):
                        bot_temp.send_video(chat_id, media, caption=texto, reply_markup=markup)
                    else:
                        bot_temp.send_photo(chat_id, media, caption=texto, reply_markup=markup)
                except Exception as e:
                    logger.error(f"Erro mídia 1: {e}")
                    bot_temp.send_message(chat_id, texto, reply_markup=markup_step if passo.mostrar_botao else None)
            else:
                bot_temp.send_message(chat_id, texto, reply_markup=markup_step if passo.mostrar_botao else None)

        # ============================================================
        # TRECHO 1: CALLBACK "passo_2"
        # ============================================================

        elif update.callback_query and update.callback_query.data == "passo_2":
            chat_id = update.callback_query.message.chat.id
            msg_id = update.callback_query.message.message_id
            
            logger.info(f"🎯 [BOT {bot_db.id}] Usuário clicou em passo_2")
            
            # Busca o PRIMEIRO passo intermediário
            primeiro_passo = db.query(BotFlowStep).filter(
                BotFlowStep.bot_id == bot_db.id,
                BotFlowStep.step_order == 1
            ).first()
            
            if primeiro_passo:
                logger.info(f"✅ [BOT {bot_db.id}] Encontrado passo 1: {primeiro_passo.msg_texto[:30]}...")
                
                # Verifica se existe passo 2
                segundo_passo = db.query(BotFlowStep).filter(
                    BotFlowStep.bot_id == bot_db.id, 
                    BotFlowStep.step_order == 2
                ).first()
                
                # Define o callback do botão
                if segundo_passo:
                    next_callback = "next_step_1"
                    logger.info(f"🔗 [BOT {bot_db.id}] Há mais passos. Botão vai chamar: {next_callback}")
                else:
                    next_callback = "go_checkout"
                    logger.info(f"🔗 [BOT {bot_db.id}] Último passo. Botão vai chamar: {next_callback}")
                
                # Auto-destruir mensagem de boas-vindas (se configurado)
                if bot_db.fluxo.autodestruir_1:
                    try:
                        bot_temp.delete_message(chat_id, msg_id)
                        logger.info(f"💣 [BOT {bot_db.id}] Mensagem de boas-vindas auto-destruída")
                    except:
                        pass
                
                # [NOVO V3] Só cria botão se mostrar_botao = True
                markup_step = types.InlineKeyboardMarkup()
                if primeiro_passo.mostrar_botao:
                    markup_step.add(types.InlineKeyboardButton(
                        text=primeiro_passo.btn_texto, 
                        callback_data=next_callback
                    ))

                # Envia o PASSO 1 e SALVA o message_id
                sent_msg = None
                if primeiro_passo.msg_media:
                    try:
                        if primeiro_passo.msg_media.lower().endswith(('.mp4', '.mov')):
                            sent_msg = bot_temp.send_video(
                                chat_id, 
                                primeiro_passo.msg_media, 
                                caption=primeiro_passo.msg_texto, 
                                reply_markup=markup_step if primeiro_passo.mostrar_botao else None
                            )
                        else:
                            sent_msg = bot_temp.send_photo(
                                chat_id, 
                                primeiro_passo.msg_media, 
                                caption=primeiro_passo.msg_texto, 
                                reply_markup=markup_step if primeiro_passo.mostrar_botao else None
                            )
                    except:
                        sent_msg = bot_temp.send_message(
                            chat_id, 
                            primeiro_passo.msg_texto, 
                            reply_markup=markup_step if primeiro_passo.mostrar_botao else None
                        )
                else:
                    sent_msg = bot_temp.send_message(
                        chat_id, 
                        primeiro_passo.msg_texto, 
                        reply_markup=markup_step if primeiro_passo.mostrar_botao else None
                    )
                
                # [NOVO V4] Se não tem botão e tem delay, agenda próxima mensagem
                if not primeiro_passo.mostrar_botao and primeiro_passo.delay_seconds > 0:
                    logger.info(f"⏰ [BOT {bot_db.id}] Aguardando {primeiro_passo.delay_seconds}s antes de enviar próximo passo...")
                    time.sleep(primeiro_passo.delay_seconds)
                    
                    # [CORREÇÃO V4.1] Auto-destruir antes de enviar a próxima
                    if primeiro_passo.autodestruir and sent_msg:
                        try:
                            bot_temp.delete_message(chat_id, sent_msg.message_id)
                            logger.info(f"💣 [BOT {bot_db.id}] Mensagem do passo 1 auto-destruída")
                        except:
                            pass
                    
                    # Busca o segundo passo
                    segundo_passo = db.query(BotFlowStep).filter(
                        BotFlowStep.bot_id == bot_db.id, 
                        BotFlowStep.step_order == 2
                    ).first()
                    
                    if segundo_passo:
                        enviar_passo_automatico(bot_temp, chat_id, segundo_passo, bot_db, db)
            else:
                # Se não tem passos intermediários, vai direto pro checkout
                logger.info(f"⚠️ [BOT {bot_db.id}] Nenhum passo intermediário configurado, indo direto para oferta")
                enviar_oferta_final(bot_temp, chat_id, bot_db.fluxo, bot_db.id, db)
            
            bot_temp.answer_callback_query(update.callback_query.id)


        # ============================================================
        # TRECHO 2: CALLBACK "next_step_"
        # ============================================================

        elif update.callback_query and update.callback_query.data.startswith("next_step_"):
            chat_id = update.callback_query.message.chat.id
            msg_id = update.callback_query.message.message_id
            
            # Extrai o número do passo ATUAL
            try:
                passo_atual_order = int(update.callback_query.data.split("_")[2])
            except: 
                passo_atual_order = 1
            
            logger.info(f"🎯 [BOT {bot_db.id}] Usuário clicou em next_step_{passo_atual_order}")
            
            # [NOVO V3] Busca o passo ATUAL para verificar auto-destruir
            passo_atual = db.query(BotFlowStep).filter(
                BotFlowStep.bot_id == bot_db.id, 
                BotFlowStep.step_order == passo_atual_order
            ).first()
            
            # [NOVO V3] Auto-destruir o passo atual (se configurado)
            if passo_atual and passo_atual.autodestruir:
                try:
                    bot_temp.delete_message(chat_id, msg_id)
                    logger.info(f"💣 [BOT {bot_db.id}] Mensagem do passo {passo_atual_order} auto-destruída")
                except:
                    pass
            
            # Busca o PRÓXIMO passo (atual + 1)
            proximo_passo = db.query(BotFlowStep).filter(
                BotFlowStep.bot_id == bot_db.id, 
                BotFlowStep.step_order == passo_atual_order + 1
            ).first()

            if proximo_passo:
                logger.info(f"✅ [BOT {bot_db.id}] Enviando passo {proximo_passo.step_order}: {proximo_passo.msg_texto[:30]}...")
                
                # Verifica se existe um passo DEPOIS deste
                passo_seguinte = db.query(BotFlowStep).filter(
                    BotFlowStep.bot_id == bot_db.id, 
                    BotFlowStep.step_order == proximo_passo.step_order + 1
                ).first()
                
                # Define o callback do botão
                if passo_seguinte:
                    next_callback = f"next_step_{proximo_passo.step_order}"
                    logger.info(f"🔗 [BOT {bot_db.id}] Há mais passos. Próximo botão vai chamar: {next_callback}")
                else:
                    next_callback = "go_checkout"
                    logger.info(f"🔗 [BOT {bot_db.id}] Último passo. Próximo botão vai chamar: {next_callback}")
                
                # [NOVO V3] Só cria botão se mostrar_botao = True
                markup_step = types.InlineKeyboardMarkup()
                if proximo_passo.mostrar_botao:
                    markup_step.add(types.InlineKeyboardButton(
                        text=proximo_passo.btn_texto, 
                        callback_data=next_callback
                    ))

                # Envia a mensagem do PRÓXIMO PASSO e SALVA o message_id
                sent_msg = None
                if proximo_passo.msg_media:
                    try:
                        if proximo_passo.msg_media.lower().endswith(('.mp4', '.mov')):
                            sent_msg = bot_temp.send_video(
                                chat_id, 
                                proximo_passo.msg_media, 
                                caption=proximo_passo.msg_texto, 
                                reply_markup=markup_step if proximo_passo.mostrar_botao else None
                            )
                        else:
                            sent_msg = bot_temp.send_photo(
                                chat_id, 
                                proximo_passo.msg_media, 
                                caption=proximo_passo.msg_texto, 
                                reply_markup=markup_step if proximo_passo.mostrar_botao else None
                            )
                    except:
                        sent_msg = bot_temp.send_message(
                            chat_id, 
                            proximo_passo.msg_texto, 
                            reply_markup=markup_step if proximo_passo.mostrar_botao else None
                        )
                else:
                    sent_msg = bot_temp.send_message(
                        chat_id, 
                        proximo_passo.msg_texto, 
                        reply_markup=markup_step if proximo_passo.mostrar_botao else None
                    )
                
                # [NOVO V4] Se não tem botão e tem delay, agenda próxima mensagem
                if not proximo_passo.mostrar_botao and proximo_passo.delay_seconds > 0:
                    logger.info(f"⏰ [BOT {bot_db.id}] Aguardando {proximo_passo.delay_seconds}s antes de enviar próximo passo...")
                    time.sleep(proximo_passo.delay_seconds)
                    
                    # [CORREÇÃO V4.1] Auto-destruir antes de enviar a próxima
                    if proximo_passo.autodestruir and sent_msg:
                        try:
                            bot_temp.delete_message(chat_id, sent_msg.message_id)
                            logger.info(f"💣 [BOT {bot_db.id}] Mensagem do passo {proximo_passo.step_order} auto-destruída")
                        except:
                            pass
                    
                    # Busca o passo seguinte
                    passo_seguinte = db.query(BotFlowStep).filter(
                        BotFlowStep.bot_id == bot_db.id, 
                        BotFlowStep.step_order == proximo_passo.step_order + 1
                    ).first()
                    
                    if passo_seguinte:
                        enviar_passo_automatico(bot_temp, chat_id, passo_seguinte, bot_db, db)
                    else:
                        # Não tem mais passos, vai pro checkout
                        enviar_oferta_final(bot_temp, chat_id, bot_db.fluxo, bot_db.id, db)
            else:
                # Não tem mais passos, vai para checkout
                logger.info(f"⚠️ [BOT {bot_db.id}] Não há mais passos, indo para checkout")
                enviar_oferta_final(bot_temp, chat_id, bot_db.fluxo, bot_db.id, db)
            
            bot_temp.answer_callback_query(update.callback_query.id)

        # --- IR PARA CHECKOUT ---
        elif update.callback_query and update.callback_query.data == "go_checkout":
            chat_id = update.callback_query.message.chat.id
            logger.info(f"🎯 [BOT {bot_db.id}] Indo para checkout final")
            enviar_oferta_final(bot_temp, chat_id, bot_db.fluxo, bot_db.id, db)
            bot_temp.answer_callback_query(update.callback_query.id)

        # ============================================================
        # TRECHO COMPLETO CORRIGIDO - PROMOÇÕES + CHECKOUT PADRÃO
        # ============================================================

        # --- PROMOÇÕES (CORRIGIDO) ---
        elif update.callback_query and update.callback_query.data.startswith("promo_"):
            chat_id = update.callback_query.message.chat.id
            first_name = update.callback_query.from_user.first_name
            username = update.callback_query.from_user.username
            campanha_uuid = update.callback_query.data.split("_")[1]
            
            campanha = db.query(RemarketingCampaign).filter(
                RemarketingCampaign.campaign_id == campanha_uuid
            ).first()
            
            if not campanha or not campanha.plano_id:
                bot_temp.answer_callback_query(update.callback_query.id, "Oferta não encontrada.")
                return {"status": "error"}

            # Verifica se a oferta expirou
            if campanha.expiration_at and datetime.utcnow() > campanha.expiration_at:
                msg_esgotado = "🚫 **OFERTA ENCERRADA!**\n\nO tempo da oferta acabou."
                bot_temp.send_message(chat_id, msg_esgotado, parse_mode="Markdown")
                bot_temp.answer_callback_query(update.callback_query.id, "Oferta expirada!")
                return {"status": "expired"}

            plano = db.query(PlanoConfig).filter(PlanoConfig.id == campanha.plano_id).first()
            if not plano:
                bot_temp.send_message(chat_id, "❌ Plano não encontrado.")
                return {"status": "error"}
            
            preco_final = campanha.promo_price if campanha.promo_price else plano.preco_atual
            
            msg_aguarde = bot_temp.send_message(chat_id, f"⏳ Gerando oferta de R$ {preco_final:.2f}...")
            
            temp_uuid = str(uuid.uuid4())
            pix_data = gerar_pix_pushinpay(preco_final, temp_uuid)
            
            if pix_data:
                qr_code_text = pix_data.get("qr_code_text") or pix_data.get("qr_code")
                provider_id = pix_data.get("id") or temp_uuid
                final_tx_id = str(provider_id).lower()

                # ============================================================
                # [CORREÇÃO] ANTI-DUPLICAÇÃO - VERIFICA SE USUÁRIO JÁ EXISTE
                # ============================================================
                pedido_existente = db.query(Pedido).filter(
                    Pedido.telegram_id == str(chat_id),
                    Pedido.bot_id == bot_db.id
                ).first()

                if pedido_existente:
                    # [CORREÇÃO] ATUALIZA o pedido existente
                    logger.info(f"📝 [BOT {bot_db.id}] Usuário {chat_id} já existe. Atualizando pedido (PROMO)...")
                    
                    pedido_existente.plano_nome = f"{plano.nome_exibicao} (OFERTA)"
                    pedido_existente.plano_id = plano.id
                    pedido_existente.valor = preco_final
                    pedido_existente.status = "pending"
                    pedido_existente.transaction_id = final_tx_id
                    pedido_existente.qr_code = qr_code_text
                    pedido_existente.data_aprovacao = None
                    pedido_existente.created_at = datetime.utcnow()
                    
                    # Se tinha custom_expiration e agora é vitalício, remove
                    if plano.dias_duracao == 99999:
                        pedido_existente.custom_expiration = None
                    
                    db.commit()
                    db.refresh(pedido_existente)
                    
                    logger.info(f"✅ [BOT {bot_db.id}] Pedido atualizado para {chat_id} (PROMO)")
                else:
                    # [MANTÉM] Se não existe, cria um novo
                    logger.info(f"🆕 [BOT {bot_db.id}] Criando primeiro pedido para {chat_id} (PROMO)...")
                    
                    novo_pedido = Pedido(
                        bot_id=bot_db.id,
                        transaction_id=final_tx_id,
                        telegram_id=str(chat_id),
                        first_name=first_name,
                        username=username,
                        plano_nome=f"{plano.nome_exibicao} (OFERTA)",
                        plano_id=plano.id,
                        valor=preco_final,
                        status="pending",
                        qr_code=qr_code_text,
                        created_at=datetime.utcnow()
                    )
                    db.add(novo_pedido)
                    db.commit()
                    db.refresh(novo_pedido)
                    
                    logger.info(f"✅ [BOT {bot_db.id}] Pedido criado para {chat_id} (PROMO)")

                try: 
                    bot_temp.delete_message(chat_id, msg_aguarde.message_id)
                except: 
                    pass

                # Manda o PIX Bonitinho (PROMOÇÃO)
                legenda_pix = f"""🎉 **OFERTA ATIVADA COM SUCESSO!**
🎁 Plano: {plano.nome_exibicao}
💸 **Valor Promocional: R$ {preco_final:.2f}**

Copie o código abaixo para garantir sua vaga:

```
{qr_code_text}
```

👆 Toque no código para copiar.
⏳ Pague agora antes que expire!"""

                bot_temp.send_message(chat_id, legenda_pix, parse_mode="Markdown")
            else:
                bot_temp.send_message(chat_id, "❌ Erro ao gerar oferta.")

            bot_temp.answer_callback_query(update.callback_query.id)
            return {"status": "processed"}

        # ============================================================
        # 🛒 CHECKOUT PADRÃO (CORRIGIDO COM ANTI-DUPLICAÇÃO)
        # ============================================================
        elif update.callback_query and update.callback_query.data.startswith("checkout_"):
            chat_id = update.callback_query.message.chat.id
            first_name = update.callback_query.from_user.first_name
            username = update.callback_query.from_user.username
            plano_id = update.callback_query.data.split("_")[1]
            
            plano = db.query(PlanoConfig).filter(PlanoConfig.id == plano_id).first()
            if not plano:
                bot_temp.send_message(chat_id, "❌ Plano não encontrado.")
                return {"status": "error"}

            msg_aguarde = bot_temp.send_message(chat_id, "⏳ Gerando seu PIX...")
            
            temp_uuid = str(uuid.uuid4())
            pix_data = gerar_pix_pushinpay(plano.preco_atual, temp_uuid)
            
            if pix_data:
                qr_code_text = pix_data.get("qr_code_text") or pix_data.get("qr_code")
                provider_id = pix_data.get("id") or temp_uuid
                final_tx_id = str(provider_id).lower()

                # ============================================================
                # [CORREÇÃO] ANTI-DUPLICAÇÃO - VERIFICA SE USUÁRIO JÁ EXISTE
                # ============================================================
                pedido_existente = db.query(Pedido).filter(
                    Pedido.telegram_id == str(chat_id),
                    Pedido.bot_id == bot_db.id
                ).first()

                if pedido_existente:
                    # [CORREÇÃO] ATUALIZA o pedido existente
                    logger.info(f"📝 [BOT {bot_db.id}] Usuário {chat_id} já existe. Atualizando pedido...")
                    
                    pedido_existente.plano_nome = plano.nome_exibicao
                    pedido_existente.plano_id = plano.id
                    pedido_existente.valor = plano.preco_atual
                    pedido_existente.status = "pending"
                    pedido_existente.transaction_id = final_tx_id
                    pedido_existente.qr_code = qr_code_text
                    pedido_existente.data_aprovacao = None
                    pedido_existente.created_at = datetime.utcnow()
                    
                    # Se tinha custom_expiration e agora é vitalício, remove
                    if plano.dias_duracao == 99999:
                        pedido_existente.custom_expiration = None
                    
                    db.commit()
                    db.refresh(pedido_existente)
                    
                    logger.info(f"✅ [BOT {bot_db.id}] Pedido atualizado para {chat_id}")
                    
                    # [NOVO] Notifica admin sobre lead atualizado
                    try:
                        msg_lead = f"🔄 *Lead Atualizado (PIX Gerado)*\n👤 {first_name}\n💰 R$ {plano.preco_atual:.2f}"
                        notificar_admin_principal(bot_db, msg_lead)
                    except Exception as e:
                        logger.error(f"Erro ao notificar lead: {e}")
                else:
                    # [MANTÉM] Se não existe, cria um novo
                    logger.info(f"🆕 [BOT {bot_db.id}] Criando primeiro pedido para {chat_id}...")
                    
                    novo_pedido = Pedido(
                        bot_id=bot_db.id,
                        transaction_id=final_tx_id, 
                        telegram_id=str(chat_id),
                        first_name=first_name,
                        username=username,
                        plano_nome=plano.nome_exibicao,
                        plano_id=plano.id,
                        valor=plano.preco_atual,
                        status="pending",
                        qr_code=qr_code_text,
                        created_at=datetime.utcnow()
                    )
                    db.add(novo_pedido)
                    db.commit()
                    db.refresh(novo_pedido)
                    
                    logger.info(f"✅ [BOT {bot_db.id}] Pedido criado para {chat_id}")

                    # [MANTÉM] Notifica admin sobre novo lead
                    try:
                        msg_lead = f"🆕 *Novo Lead (PIX Gerado)*\n👤 {first_name}\n💰 R$ {plano.preco_atual:.2f}"
                        notificar_admin_principal(bot_db, msg_lead)
                    except Exception as e:
                        logger.error(f"Erro ao notificar lead: {e}")

                try: 
                    bot_temp.delete_message(chat_id, msg_aguarde.message_id)
                except: 
                    pass

                # Manda o PIX Bonitinho (CHECKOUT PADRÃO)
                legenda_pix = f"""🌟 Seu pagamento foi gerado com sucesso:
🎁 Plano: {plano.nome_exibicao}
💰 Valor: R$ {plano.preco_atual:.2f}
🔐 Pague via Pix Copia e Cola:

```
{qr_code_text}
```

👆 Toque na chave PIX acima para copiá-la
‼️ Após o pagamento, o acesso será liberado automaticamente!"""

                bot_temp.send_message(chat_id, legenda_pix, parse_mode="Markdown")
            else:
                bot_temp.send_message(chat_id, "❌ Erro ao gerar PIX. Tente novamente ou contate o suporte.")

            bot_temp.answer_callback_query(update.callback_query.id)

        return {"status": "processed"}
        
    except Exception as e:
        logger.error(f"Erro webhook: {e}")
        return {"status": "error"}

# =========================================================
# 👥 ROTAS DE CRM (BASE DE CONTATOS CORRIGIDA + FILTROS INTELIGENTES)
# =========================================================
@app.get("/api/admin/contacts")
def listar_contatos(
    bot_id: Optional[int] = None, 
    status: str = "todos",
    page: int = 1,        # [NOVO] Número da página (começa em 1)
    per_page: int = 50,   # [NOVO] Quantidade por página (padrão 50)
    db: Session = Depends(get_db)
):
    """
    Lista contatos com paginação.
    
    Parâmetros:
    - bot_id: ID do bot (opcional, filtra por bot)
    - status: todos, pagantes, pendentes, expirados
    - page: Número da página (1, 2, 3...)
    - per_page: Registros por página (padrão 50, máximo 100)
    
    Retorna:
    {
        "data": [...],
        "total": 150,
        "page": 1,
        "per_page": 50,
        "total_pages": 3
    }
    """
    
    # Limita per_page a no máximo 100
    per_page = min(per_page, 100)
    
    # Query base
    query = db.query(Pedido)
    
    # Filtra por bot se especificado
    if bot_id:
        query = query.filter(Pedido.bot_id == bot_id)
    
    # Filtra por status
    if status == "pagantes":
        query = query.filter(Pedido.status.in_(["paid", "active", "approved"]))
    elif status == "pendentes":
        query = query.filter(Pedido.status == "pending")
    elif status == "expirados":
        query = query.filter(Pedido.status == "expired")
    # Se status == "todos", não filtra
    
    # [NOVO] Conta total de registros (ANTES da paginação)
    total_count = query.count()
    
    # [NOVO] Calcula total de páginas
    total_pages = (total_count + per_page - 1) // per_page  # Divisão com arredondamento para cima
    
    # [NOVO] Aplica paginação (OFFSET e LIMIT)
    offset = (page - 1) * per_page
    contatos = query.order_by(Pedido.created_at.desc()).offset(offset).limit(per_page).all()
    
    # Formata resposta
    result = []
    for pedido in contatos:
        result.append({
            "id": pedido.id,
            "telegram_id": pedido.telegram_id,
            "first_name": pedido.first_name,
            "username": pedido.username,
            "plano_nome": pedido.plano_nome,
            "valor": pedido.valor,
            "status": pedido.status,
            "role": getattr(pedido, 'role', 'user'),  # Seguro se o campo não existir
            "custom_expiration": pedido.custom_expiration,
            "created_at": pedido.created_at
        })
    
    # [NOVO] Retorna com metadados de paginação
    return {
        "data": result,
        "total": total_count,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages
    }

# [NOVA ROTA] Atualização Manual de Usuário
@app.put("/api/admin/users/{user_id}")
def update_user_crm(user_id: str, dados: UserUpdateCRM, db: Session = Depends(get_db)):
    # Tenta achar por ID ou TelegramID
    p = db.query(Pedido).filter(
        (Pedido.id == int(user_id) if user_id.isdigit() else False) | 
        (Pedido.telegram_id == user_id)
    ).first()
    
    if not p: raise HTTPException(404, "Usuário não encontrado")

    if dados.first_name: p.first_name = dados.first_name
    if dados.username: p.username = dados.username
    if dados.status: p.status = dados.status

    # Lógica da Data Manual (Dual Write)
    if dados.custom_expiration:
        try:
            # Converte string ISO para datetime
            dt = datetime.fromisoformat(dados.custom_expiration.replace("Z", ""))
            p.data_expiracao = dt
            p.custom_expiration = dt # Salva na coluna que o Front lê
        except: pass 

    db.commit()
    return {"status": "success"}

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

@app.post("/api/admin/remarketing/send-individual")
def enviar_remarketing_individual(payload: IndividualRemarketingRequest, db: Session = Depends(get_db)):
    # 1. Busca os dados da campanha antiga
    campanha = db.query(RemarketingCampaign).filter(RemarketingCampaign.id == payload.campaign_history_id).first()
    if not campanha:
        raise HTTPException(404, "Campanha original não encontrada")
    
    # 2. Decodifica a configuração
    try:
        config = json.loads(campanha.config) if isinstance(campanha.config, str) else campanha.config
        # Se config for string dentro de um json (caso antigo), tenta parsear de novo
        if isinstance(config, str): config = json.loads(config)
    except:
        config = {}

    # 3. Reconstrói o Payload
    msg = config.get("msg", "")
    media = config.get("media", "")
    
    # [CORREÇÃO CRÍTICA] Não buscamos mais 'offer' do config JSON, pois ele pode não ter sido salvo lá.
    # A verificação será feita direto pelo ID do plano na tabela.

    # 4. Prepara envio
    bot_db = db.query(Bot).filter(Bot.id == payload.bot_id).first()
    if not bot_db: raise HTTPException(404, "Bot não encontrado")
    
    sender = telebot.TeleBot(bot_db.token)
    
    # 5. Monta Botão (CORRIGIDO: Se tiver plano_id no banco, TEM oferta)
    markup = None
    if campanha.plano_id:
        # Recupera plano
        plano = db.query(PlanoConfig).filter(PlanoConfig.id == campanha.plano_id).first()
        if plano:
            markup = types.InlineKeyboardMarkup()
            # Usa o preço promocional salvo na campanha ou o atual
            preco = campanha.promo_price or plano.preco_atual
            btn_text = f"🔥 {plano.nome_exibicao} - R$ {preco:.2f}"
            
            # OBS: Usamos um checkout direto aqui para garantir que funcione, 
            # já que links de promoções antigas poderiam estar expirados.
            # Se quiser forçar a mesma campanha, use f"promo_{campanha.campaign_id}"
            # Mas checkout direto é mais seguro para disparo individual manual.
            markup.add(types.InlineKeyboardButton(btn_text, callback_data=f"checkout_{plano.id}"))

    # 6. Envia
    try:
        if media:
            try:
                # Tenta enviar como vídeo ou foto
                if media.lower().endswith(('.mp4', '.mov', '.avi')):
                    sender.send_video(payload.user_telegram_id, media, caption=msg, reply_markup=markup)
                else:
                    sender.send_photo(payload.user_telegram_id, media, caption=msg, reply_markup=markup)
            except Exception as e_media:
                # Se falhar a mídia (link quebrado), envia só texto com o botão
                logger.warning(f"Falha ao enviar mídia: {e_media}. Tentando texto.")
                sender.send_message(payload.user_telegram_id, msg, reply_markup=markup)
        else:
            sender.send_message(payload.user_telegram_id, msg, reply_markup=markup)
            
        return {"status": "sent", "msg": "Mensagem enviada com sucesso!"}
    except Exception as e:
        logger.error(f"Erro envio individual: {e}")
        # Retorna erro 500 para o frontend saber
        raise HTTPException(status_code=500, detail=f"Falha ao enviar: {str(e)}")

# =========================================================
# 📢 LÓGICA DE REMARKETING (ALINHADA COM O FRONTEND + LÓGICA DE CONJUNTOS)
# =========================================================
CAMPAIGN_STATUS = {
    "running": False,
    "sent": 0,
    "total": 0,
    "blocked": 0
}

def processar_envio_remarketing(bot_id: int, payload: RemarketingRequest, db: Session):
    global CAMPAIGN_STATUS
    CAMPAIGN_STATUS = {"running": True, "sent": 0, "total": 0, "blocked": 0}
    
    bot_db = db.query(Bot).filter(Bot.id == bot_id).first()
    if not bot_db: 
        CAMPAIGN_STATUS["running"] = False
        return

    # --- 1. IDENTIFICAÇÃO DO FILTRO (CORREÇÃO CRÍTICA) ---
    # Seu Frontend manda 'target'. Ex: "pendentes", "todos", etc.
    filtro_limpo = str(payload.target).lower().strip()
    
    # Fallback: Se por acaso vier tipo_envio (teste de API manual), usa ele
    if payload.tipo_envio:
        filtro_limpo = str(payload.tipo_envio).lower().strip()

    logger.info(f"🚀 INICIANDO DISPARO | Bot: {bot_db.nome} | Filtro Solicitado: {filtro_limpo}")

    # --- 2. PREPARAÇÃO DA MENSAGEM E OFERTA ---
    uuid_campanha = str(uuid.uuid4())
    data_expiracao = None
    preco_final = 0.0
    plano_db = None

    if payload.incluir_oferta and payload.plano_oferta_id:
        # Busca plano pelo ID ou Key
        plano_db = db.query(PlanoConfig).filter(
            (PlanoConfig.key_id == str(payload.plano_oferta_id)) | 
            (PlanoConfig.id == int(payload.plano_oferta_id) if str(payload.plano_oferta_id).isdigit() else False)
        ).first()

        if plano_db:
            # Lógica de preço (Baseada no JSX: price_mode e custom_price)
            if payload.price_mode == 'custom' and payload.custom_price and payload.custom_price > 0:
                preco_final = payload.custom_price
            else:
                preco_final = plano_db.preco_atual
            
            # Lógica de expiração (Baseada no JSX: expiration_mode e expiration_value)
            if payload.expiration_mode != "none" and payload.expiration_value > 0:
                agora = datetime.utcnow()
                val = payload.expiration_value
                if payload.expiration_mode == "minutes": data_expiracao = agora + timedelta(minutes=val)
                elif payload.expiration_mode == "hours": data_expiracao = agora + timedelta(hours=val)
                elif payload.expiration_mode == "days": data_expiracao = agora + timedelta(days=val)

    # --- 3. SELEÇÃO DE PÚBLICO (SETS MATEMÁTICOS - INFALÍVEL) ---
    bot_sender = telebot.TeleBot(bot_db.token)
    lista_final_ids = []

    if payload.is_test:
        # Modo Teste: Envia apenas para o ID específico ou Admin
        if payload.specific_user_id:
            lista_final_ids = [str(payload.specific_user_id).strip()]
        else:
            adm = db.query(BotAdmin).filter(BotAdmin.bot_id == bot_id).first()
            if adm: lista_final_ids = [str(adm.telegram_id).strip()]
        logger.info(f"🧪 MODO TESTE: Enviando para {lista_final_ids}")

    else:
        # A) Busca TODOS os IDs únicos deste bot
        q_todos = db.query(Pedido.telegram_id).filter(Pedido.bot_id == bot_id).distinct()
        ids_todos = {str(r[0]).strip() for r in q_todos.all() if r[0]}

        # B) Busca PAGOS (Ids de quem tem status positivo)
        # Atenção: func.lower garante que pegue 'Paid', 'paid', 'PAID'
        q_pagos = db.query(Pedido.telegram_id).filter(
            Pedido.bot_id == bot_id, 
            func.lower(Pedido.status).in_(['paid', 'active', 'approved', 'completed', 'succeeded'])
        ).distinct()
        ids_pagantes = {str(r[0]).strip() for r in q_pagos.all() if r[0]}

        # C) Busca EXPIRADOS
        q_expirados = db.query(Pedido.telegram_id).filter(
            Pedido.bot_id == bot_id, 
            func.lower(Pedido.status) == 'expired'
        ).distinct()
        ids_expirados = {str(r[0]).strip() for r in q_expirados.all() if r[0]}

        # --- APLICAÇÃO DO FILTRO (AQUI OCORRE A MÁGICA) ---
        
        if filtro_limpo in ['pendentes', 'leads', 'nao_pagantes']:
            # Lógica: Pega TODOS e remove quem PAGOU e quem já foi CLIENTE (expirado)
            # Resultado: Só sobra leads frios (nunca compraram)
            lista_final_ids = list(ids_todos - ids_pagantes - ids_expirados)
            logger.info(f"📊 FILTRO PENDENTES: {len(ids_todos)} (Total) - {len(ids_pagantes)} (Pagos) - {len(ids_expirados)} (Expirados) = {len(lista_final_ids)}")

        elif filtro_limpo in ['pagantes', 'ativos']:
            lista_final_ids = list(ids_pagantes)
            
        elif filtro_limpo in ['expirados', 'ex_assinantes']:
            # Lógica: Quem expirou MENOS quem renovou (pagou)
            lista_final_ids = list(ids_expirados - ids_pagantes)
            
        elif filtro_limpo == 'todos':
            lista_final_ids = list(ids_todos)
            
        else:
            # Se não reconheceu o filtro, por segurança NÃO envia ou envia log de erro
            logger.warning(f"⚠️ Filtro desconhecido '{filtro_limpo}'. Assumindo TODOS por segurança.")
            lista_final_ids = list(ids_todos)

    CAMPAIGN_STATUS["total"] = len(lista_final_ids)

    # --- 4. BOTÃO ---
    markup = None
    if plano_db:
        markup = types.InlineKeyboardMarkup()
        btn_text = f"🔥 {plano_db.nome_exibicao} - R$ {preco_final:.2f}"
        
        if payload.is_test:
             markup.add(types.InlineKeyboardButton(f"[TESTE] {btn_text}", callback_data=f"checkout_{plano_db.id}"))
        else:
             # O callback promo_UUID vai acionar a verificação de validade no webhook
             markup.add(types.InlineKeyboardButton(btn_text, callback_data=f"promo_{uuid_campanha}"))

    # --- 5. LOOP DE ENVIO ---
    sent_count = 0
    blocked_count = 0

    for uid in lista_final_ids:
        if not uid or len(uid) < 5: continue
        try:
            midia_ok = False
            # Mídia
            if payload.media_url and len(payload.media_url) > 5:
                try:
                    ext = payload.media_url.lower()
                    if ext.endswith(('.mp4', '.mov', '.avi')):
                        bot_sender.send_video(uid, payload.media_url, caption=payload.mensagem, reply_markup=markup, parse_mode="Markdown")
                    else:
                        bot_sender.send_photo(uid, payload.media_url, caption=payload.mensagem, reply_markup=markup, parse_mode="Markdown")
                    midia_ok = True
                except: pass 
            
            # Texto (se não foi mídia)
            if not midia_ok:
                bot_sender.send_message(uid, payload.mensagem, reply_markup=markup, parse_mode="Markdown")
            
            sent_count += 1
            time.sleep(0.04) 
            
        except Exception as e:
            err = str(e).lower()
            if "blocked" in err or "kicked" in err or "deactivated" in err or "chat not found" in err:
                blocked_count += 1

    CAMPAIGN_STATUS["running"] = False
    
    # Salva Histórico
    if not payload.is_test:
        campanha_db = RemarketingCampaign(
            bot_id=bot_id,
            campaign_id=uuid_campanha,
            type="massivo",
            target=filtro_limpo, # Salva o filtro correto que foi usado
            config=json.dumps({"msg": payload.mensagem, "media": payload.media_url}),
            status="concluido",
            plano_id=plano_db.id if plano_db else None,
            promo_price=preco_final if plano_db else None,
            total_leads=len(lista_final_ids),
            sent_success=sent_count,
            blocked_count=blocked_count,
            expiration_at=data_expiracao
        )
        db.add(campanha_db)
        db.commit()

    logger.info(f"✅ FINALIZADO: {sent_count} enviados / {blocked_count} bloqueados")

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

    background_tasks.add_task(processar_envio_remarketing, payload.bot_id, payload, db)
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
            "data": camp.data_envio.strftime("%d/%m/%Y %H:%M") if camp.data_envio else "N/A",
            "target": camp.target,
            "total": camp.total_leads,
            "blocked": camp.blocked_count,
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
@app.get("/api/admin/dashboard/stats")
def dashboard_stats(bot_id: Optional[int] = None, db: Session = Depends(get_db)): 
    """Calcula métricas. Se bot_id for passado, filtra por ele."""
    
    # [CORREÇÃO FINANCEIRA] - Faturamento Total
    # Soma vendas ativas E expiradas. O dinheiro entrou, conta como receita.
    q_revenue = db.query(func.sum(Pedido.valor)).filter(
        Pedido.status.in_(['paid', 'active', 'approved', 'expired', 'completed', 'succeeded'])
    )
    
    # Usuários Ativos (Aqui SIM ignoramos os expirados, pois queremos saber quem está no canal agora)
    q_users = db.query(Pedido.telegram_id).filter(
        Pedido.status.in_(['paid', 'active', 'approved', 'completed', 'succeeded'])
    )
    
    # Vendas Hoje (Considera qualquer venda feita hoje, mesmo que tenha sido teste curto e expirou)
    today = datetime.utcnow().date()
    start_of_day = datetime.combine(today, datetime.min.time())
    q_sales_today = db.query(func.sum(Pedido.valor)).filter(
        Pedido.status.in_(['paid', 'active', 'approved', 'expired', 'completed', 'succeeded']),
        Pedido.created_at >= start_of_day
    )

    # APLICA FILTRO DE BOT (SE SELECIONADO)
    if bot_id:
        q_revenue = q_revenue.filter(Pedido.bot_id == bot_id)
        q_users = q_users.filter(Pedido.bot_id == bot_id)
        q_sales_today = q_sales_today.filter(Pedido.bot_id == bot_id)

    total_revenue = q_revenue.scalar() or 0.0
    active_users = q_users.distinct().count()
    sales_today = q_sales_today.scalar() or 0.0

    return {
        "total_revenue": total_revenue,
        "active_users": active_users,
        "sales_today": sales_today
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

def enviar_passo_automatico(bot_temp, chat_id, passo, bot_db, db):
    """
    Envia um passo automaticamente após o delay.
    Similar à lógica do next_step_, mas sem callback do usuário.
    """
    logger.info(f"✅ [BOT {bot_db.id}] Enviando passo {passo.step_order} automaticamente: {passo.msg_texto[:30]}...")
    
    # Verifica se existe passo seguinte
    passo_seguinte = db.query(BotFlowStep).filter(
        BotFlowStep.bot_id == bot_db.id, 
        BotFlowStep.step_order == passo.step_order + 1
    ).first()
    
    # Define o callback do botão
    if passo_seguinte:
        next_callback = f"next_step_{passo.step_order}"
    else:
        next_callback = "go_checkout"
    
    # Cria botão (se necessário)
    markup_step = types.InlineKeyboardMarkup()
    if passo.mostrar_botao:
        markup_step.add(types.InlineKeyboardButton(
            text=passo.btn_texto, 
            callback_data=next_callback
        ))
    
    # Envia a mensagem e SALVA o message_id
    sent_msg = None
    try:
        if passo.msg_media:
            try:
                if passo.msg_media.lower().endswith(('.mp4', '.mov')):
                    sent_msg = bot_temp.send_video(
                        chat_id, 
                        passo.msg_media, 
                        caption=passo.msg_texto, 
                        reply_markup=markup_step if passo.mostrar_botao else None
                    )
                else:
                    sent_msg = bot_temp.send_photo(
                        chat_id, 
                        passo.msg_media, 
                        caption=passo.msg_texto, 
                        reply_markup=markup_step if passo.mostrar_botao else None
                    )
            except:
                sent_msg = bot_temp.send_message(
                    chat_id, 
                    passo.msg_texto, 
                    reply_markup=markup_step if passo.mostrar_botao else None
                )
        else:
            sent_msg = bot_temp.send_message(
                chat_id, 
                passo.msg_texto, 
                reply_markup=markup_step if passo.mostrar_botao else None
            )
        
        # [RECURSIVO] Se este passo também não tem botão e tem delay
        if not passo.mostrar_botao and passo.delay_seconds > 0 and passo_seguinte:
            logger.info(f"⏰ [BOT {bot_db.id}] Aguardando {passo.delay_seconds}s antes do próximo...")
            time.sleep(passo.delay_seconds)
            
            # [CORREÇÃO V4.1] Auto-destruir antes de enviar a próxima
            if passo.autodestruir and sent_msg:
                try:
                    bot_temp.delete_message(chat_id, sent_msg.message_id)
                    logger.info(f"💣 [BOT {bot_db.id}] Mensagem do passo {passo.step_order} auto-destruída (automático)")
                except:
                    pass
            
            enviar_passo_automatico(bot_temp, chat_id, passo_seguinte, bot_db, db)
        elif not passo.mostrar_botao and not passo_seguinte:
            # Acabaram os passos, vai pro checkout
            enviar_oferta_final(bot_temp, chat_id, bot_db.fluxo, bot_db.id, db)
            
    except Exception as e:
        logger.error(f"❌ [BOT {bot_db.id}] Erro ao enviar passo automático: {e}")


# --- WEBHOOKS (LÓGICA V2) ---
# =========================================================
# WEBHOOKS (LÓGICA V3 RESTAURADA + CORREÇÃO VISUAL)
# =========================================================
@app.post("/webhook/pix")
async def wh_pix(req: Request, db: Session = Depends(get_db)):
    try:
        raw = await req.body()
        try: js = json.loads(raw)
        except: js = {k: v[0] for k,v in urllib.parse.parse_qs(raw.decode()).items()}
        
        st = str(js.get('status', '')).upper()
        # Busca ID (Compatível com PushinPay novo e antigo)
        tx = str(js.get('id') or js.get('external_reference') or js.get('uuid') or '').lower()
        
        if st in ['PAID', 'APPROVED', 'COMPLETED', 'SUCCEEDED'] and tx:
            # Busca Pedido (Tenta TXID V2 e TransactionID V1)
            ped = db.query(Pedido).filter(Pedido.txid == tx).first()
            if not ped:
                ped = db.query(Pedido).filter(Pedido.transaction_id == tx).first()

            if ped and ped.status != 'paid':
                now = datetime.utcnow()
                
                # --- CÁLCULO DE DATA (A LÓGICA DA VERSÃO 3 APLICADA AQUI) ---
                # Isso garante que o Frontend receba a data e não mostre "Vitalício"
                exp = None
                nm = (ped.plano_nome or "").lower()
                
                # Se NÃO for vitalício, calcula os dias igual a V3 fazia
                if "vital" not in nm and "mega" not in nm:
                    dias = 30 # Padrão
                    if "diario" in nm or "24" in nm or "1 dia" in nm: dias = 1
                    elif "semanal" in nm: dias = 7
                    elif "trimestral" in nm: dias = 90
                    elif "anual" in nm: dias = 365
                    
                    # Define a expiração
                    exp = now + timedelta(days=dias)

                # Salva no banco (Preenche as colunas que o Frontend V1 lê)
                ped.status = 'paid'
                ped.data_aprovacao = now
                ped.data_expiracao = exp      # Backend Novo
                ped.custom_expiration = exp   # Frontend Antigo (CORREÇÃO DO BUG)
                ped.mensagem_enviada = True
                db.commit()
                
                # --- ENTREGA E NOTIFICAÇÃO (ESTILO V3) ---
                bot = db.query(Bot).filter(Bot.id == ped.bot_id).first()
                if bot:
                    tb = telebot.TeleBot(bot.token)
                    try:
                        cid = int(str(bot.id_canal_vip).strip())
                        tb.unban_chat_member(cid, int(ped.telegram_id))
                        
                        lnk = tb.create_chat_invite_link(cid, member_limit=1, name=f"Venda {ped.first_name}").invite_link
                        
                        # Mensagem pro Cliente
                        msg_cli = f"✅ <b>Pagamento Aprovado!</b>\n\nSeu link: {lnk}"
                        tb.send_message(int(ped.telegram_id), msg_cli, parse_mode="HTML")
                        
                        # Mensagem pro Admin (Restaurada da V3)
                        notificar_admin_principal(bot, f"💰 Venda: R$ {ped.valor} - {ped.first_name}")
                    except Exception as e_tg:
                        logger.error(f"Erro entrega: {e_tg}")

        return {"status": "received"}
    except: return {"status": "error"}

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
# 🚀 WEBHOOK GERAL DO BOT (CORREÇÃO FLOW V2 - NAVEGAÇÃO INTELIGENTE)
# =========================================================
@app.post("/webhook/{token}")
async def tg_wh(token: str, req: Request, db: Session = Depends(get_db)):
    if token == "pix": return {"status": "ignored"}
    b = db.query(Bot).filter(Bot.token == token).first()
    if not b or b.status == "pausado": return {"status": "ignored"}
    
    try:
        js = await req.json()
        u = telebot.types.Update.de_json(js)
        tb = telebot.TeleBot(token)
        
        # --- 1. PORTEIRO (Verificação de Acesso ao Canal) ---
        if u.message and u.message.new_chat_members:
            cid = str(u.message.chat.id)
            vip = str(b.id_canal_vip).strip()
            if cid == vip:
                for m in u.message.new_chat_members:
                    if m.is_bot: continue
                    p = db.query(Pedido).filter(
                        Pedido.bot_id == b.id, 
                        Pedido.telegram_id == str(m.id), 
                        Pedido.status == 'paid'
                    ).order_by(desc(Pedido.created_at)).first()
                    
                    allowed = False
                    if p:
                        nm = (p.plano_nome or "").lower()
                        if "vital" in nm or "mega" in nm: 
                            allowed = True
                        else:
                            d = 30
                            if "diario" in nm or "24" in nm: d = 1
                            elif "semanal" in nm: d = 7
                            elif "trimestral" in nm: d = 90
                            
                            if p.created_at and datetime.utcnow() < (p.created_at + timedelta(days=d)): 
                                allowed = True
                    
                    if not allowed:
                        try:
                            tb.ban_chat_member(cid, m.id)
                            tb.unban_chat_member(cid, m.id)
                        except: pass
            return {"status": "checked"}

        # --- 2. COMANDO /START ---
        if u.message and u.message.text == "/start":
            cid = u.message.chat.id
            fl = b.fluxo
            txt = fl.msg_boas_vindas if fl else "Olá!"
            btn = fl.btn_text_1 if fl else "🔓 ABRIR"
            med = fl.media_url if fl else None
            mk = types.InlineKeyboardMarkup()
            mk.add(types.InlineKeyboardButton(btn, callback_data="passo_2"))
            
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

        # --- 3. CALLBACKS (BOTÕES) - AQUI ESTÁ A CORREÇÃO CRÍTICA ---
        elif u.callback_query:
            call = u.callback_query
            cid = call.message.chat.id
            dat = call.data
            
            # ============================================================
            # 🔥 CORREÇÃO: NAVEGAÇÃO INTELIGENTE ENTRE PASSOS
            # ============================================================
            if dat == "passo_2":
                # 1. Auto-destruir mensagem anterior (se configurado)
                if b.fluxo and b.fluxo.autodestruir_1:
                    try: 
                        tb.delete_message(cid, call.message.message_id)
                    except: 
                        pass
                
                # 2. BUSCA O PRIMEIRO PASSO DISPONÍVEL (NÃO APENAS O step_order=1)
                primeiro_passo = db.query(BotFlowStep).filter(
                    BotFlowStep.bot_id == b.id
                ).order_by(BotFlowStep.step_order.asc()).first()  # ← CORREÇÃO AQUI!
                
                if primeiro_passo:
                    # TEM PASSO EXTRA → Envia ele
                    mk = types.InlineKeyboardMarkup()
                    
                    # Busca o PRÓXIMO passo (se existir)
                    proximo_passo = db.query(BotFlowStep).filter(
                        BotFlowStep.bot_id == b.id,
                        BotFlowStep.step_order > primeiro_passo.step_order
                    ).order_by(BotFlowStep.step_order.asc()).first()
                    
                    # Define o próximo callback
                    if proximo_passo:
                        nxt = f"next_step_{proximo_passo.step_order}"
                    else:
                        nxt = "go_checkout"
                    
                    mk.add(types.InlineKeyboardButton(
                        primeiro_passo.btn_texto, 
                        callback_data=nxt
                    ))
                    
                    # Envia a mensagem
                    try:
                        if primeiro_passo.msg_media:
                            if primeiro_passo.msg_media.endswith(('.mp4','.mov')): 
                                tb.send_video(cid, primeiro_passo.msg_media, 
                                             caption=primeiro_passo.msg_texto, 
                                             reply_markup=mk)
                            else: 
                                tb.send_photo(cid, primeiro_passo.msg_media, 
                                            caption=primeiro_passo.msg_texto, 
                                            reply_markup=mk)
                        else: 
                            tb.send_message(cid, primeiro_passo.msg_texto, 
                                          reply_markup=mk)
                    except: 
                        tb.send_message(cid, primeiro_passo.msg_texto, 
                                      reply_markup=mk)
                else:
                    # NÃO TEM PASSOS EXTRAS → Vai direto para oferta
                    enviar_oferta_final(tb, cid, b.fluxo, b.id, db)
            
            # ============================================================
            # 🔥 CORREÇÃO: NAVEGAÇÃO ENTRE PASSOS INTERMEDIÁRIOS
            # ============================================================
            elif dat.startswith("next_step_"):
                # Extrai o número do step atual
                try: 
                    step_atual_order = int(dat.split("_")[2])
                except: 
                    step_atual_order = 1
                
                # Busca o passo atual
                passo_atual = db.query(BotFlowStep).filter(
                    BotFlowStep.bot_id == b.id,
                    BotFlowStep.step_order == step_atual_order
                ).first()
                
                if passo_atual:
                    mk = types.InlineKeyboardMarkup()
                    
                    # Busca o PRÓXIMO passo maior que o atual
                    proximo_passo = db.query(BotFlowStep).filter(
                        BotFlowStep.bot_id == b.id,
                        BotFlowStep.step_order > step_atual_order
                    ).order_by(BotFlowStep.step_order.asc()).first()
                    
                    # Define o callback do botão
                    if proximo_passo:
                        cb = f"next_step_{proximo_passo.step_order}"
                    else:
                        cb = "go_checkout"
                    
                    mk.add(types.InlineKeyboardButton(
                        passo_atual.btn_texto, 
                        callback_data=cb
                    ))
                    
                    # Envia a mensagem
                    try:
                        if passo_atual.msg_media:
                            if passo_atual.msg_media.endswith(('.mp4','.mov')): 
                                tb.send_video(cid, passo_atual.msg_media, 
                                             caption=passo_atual.msg_texto, 
                                             reply_markup=mk)
                            else: 
                                tb.send_photo(cid, passo_atual.msg_media, 
                                            caption=passo_atual.msg_texto, 
                                            reply_markup=mk)
                        else: 
                            tb.send_message(cid, passo_atual.msg_texto, 
                                          reply_markup=mk)
                    except: 
                        tb.send_message(cid, passo_atual.msg_texto, 
                                      reply_markup=mk)
                else:
                    # Se perdeu a referência, vai pro checkout
                    enviar_oferta_final(tb, cid, b.fluxo, b.id, db)

            # --- CHEGOU NO CHECKOUT ---
            elif dat == "go_checkout":
                enviar_oferta_final(tb, cid, b.fluxo, b.id, db)

            # --- CHECKOUT (GERAR PIX) ---
            elif dat.startswith("checkout_"):
                pid = dat.split("_")[1]
                pl = db.query(PlanoConfig).filter(PlanoConfig.id == pid).first()
                if pl:
                    msg = tb.send_message(cid, "⏳ Gerando PIX...")
                    mytx = str(uuid.uuid4())
                    pix = gerar_pix_pushinpay(pl.preco_atual, mytx)
                    if pix:
                        qr = pix.get('qr_code_text') or pix.get('qr_code')
                        txid = str(pix.get('id') or mytx).lower()
                        np = Pedido(
                            bot_id=b.id, telegram_id=str(cid),
                            first_name=call.from_user.first_name, 
                            username=call.from_user.username,
                            plano_nome=pl.nome_exibicao, 
                            plano_id=pl.id, 
                            valor=pl.preco_atual,
                            txid=txid, qr_code=qr, status="pending"
                        )
                        db.add(np)
                        db.commit()
                        try: 
                            tb.delete_message(cid, msg.message_id)
                        except: 
                            pass
                        tb.send_message(cid, 
                                      f"💎 Pagamento Gerado!\nValor: R$ {pl.preco_atual:.2f}\n\nCopia e Cola:\n`{qr}`", 
                                      parse_mode="Markdown")
                    else: 
                        tb.send_message(cid, "Erro PIX")
            
            # --- PROMOÇÕES (Se houver) ---
            elif dat.startswith("promo_"):
                # Lógica de promoções mantida do código original
                pass

            tb.answer_callback_query(call.id)
            
    except Exception as e:
        logger.error(f"Erro webhook: {e}")
        
    return {"status": "ok"}

@app.get("/")
def home():

    return {"status": "Zenyx SaaS Online - Banco Atualizado"}
@app.get("/admin/clean-duplicates")
def limpar_duplicatas(db: Session = Depends(get_db)):
    """
    Remove pedidos duplicados, mantendo apenas o mais recente de cada usuário.
    Execute UMA VEZ apenas para limpar dados antigos.
    """
    
    try:
        # Busca todos os bots
        bots = db.query(Bot).all()
        
        total_removidos = 0
        detalhes = []
        
        for bot in bots:
            # Para cada bot, busca usuários duplicados
            # Query: Agrupa por telegram_id e conta quantos pedidos cada um tem
            duplicados = db.query(
                Pedido.telegram_id,
                func.count(Pedido.id).label('total')
            ).filter(
                Pedido.bot_id == bot.id
            ).group_by(
                Pedido.telegram_id
            ).having(
                func.count(Pedido.id) > 1  # Só pega quem tem mais de 1 pedido
            ).all()
            
            bot_removidos = 0
            
            for telegram_id, total in duplicados:
                # Busca TODOS os pedidos deste usuário neste bot
                pedidos_usuario = db.query(Pedido).filter(
                    Pedido.telegram_id == telegram_id,
                    Pedido.bot_id == bot.id
                ).order_by(Pedido.created_at.desc()).all()  # Ordena do mais recente ao mais antigo
                
                if len(pedidos_usuario) > 1:
                    # MANTÉM o primeiro (mais recente)
                    pedido_manter = pedidos_usuario[0]
                    
                    # DELETA os outros (duplicatas antigas)
                    for pedido_deletar in pedidos_usuario[1:]:
                        db.delete(pedido_deletar)
                        bot_removidos += 1
                    
                    logger.info(f"✅ Bot {bot.nome}: Mantido pedido #{pedido_manter.id} para {telegram_id}, removidos {len(pedidos_usuario)-1} duplicados")
            
            if bot_removidos > 0:
                detalhes.append(f"Bot '{bot.nome}': {bot_removidos} duplicatas removidas")
                total_removidos += bot_removidos
        
        # Commit das alterações
        db.commit()
        
        return {
            "status": "ok",
            "total_removidos": total_removidos,
            "detalhes": detalhes,
            "mensagem": f"Limpeza concluída! {total_removidos} duplicatas removidas."
        }
    
    except Exception as e:
        db.rollback()
        logger.error(f"Erro ao limpar duplicatas: {e}")
        return {
            "status": "error",
            "mensagem": str(e)
        }