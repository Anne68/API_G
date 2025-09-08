from fastapi import FastAPI, HTTPException, Depends, Form
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from datetime import datetime, timedelta
import pymysql

app = FastAPI()

# === CONFIGURATION JWT ===
SECRET_KEY = "ma_cle_secrete_super_secure"  # À stocker dans un .env en production
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60  # Durée du token

# OAuth2 Bearer
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# === Génération de JWT ===
def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# === Vérification du JWT ===
def verify_token(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Token invalide")
        return username
    except JWTError:
        raise HTTPException(status_code=401, detail="Token invalide ou expiré")

# === Endpoint de login sécurisé ===
@app.post("/token")
def login(username: str = Form(...), password: str = Form(...)):
    if username != "admin" or password != "admin":
        raise HTTPException(status_code=401, detail="Nom d'utilisateur ou mot de passe invalide")
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": username}, expires_delta=access_token_expires)
    return {"access_token": access_token, "token_type": "bearer"}

# === Connexion à la base ===
def connect_to_db():
    return pymysql.connect(
        host="127.0.0.1",
        port=3307,
        user="root",
        password="root",
        database="games_db",
        cursorclass=pymysql.cursors.DictCursor
    )

@app.get("/")
def home():
    return {"message": "Bienvenue sur l'API des jeux vidéo avec JWT !"}

@app.get("/games", dependencies=[Depends(verify_token)])
def get_all_games():
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM games")
        games = cursor.fetchall()
        cursor.close()
        conn.close()
        if not games:
            raise HTTPException(status_code=404, detail="Aucun jeu trouvé.")
        return {"games": games}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur interne : {str(e)}")

@app.get("/games/title/{game_title}", dependencies=[Depends(verify_token)])
def get_game_by_title(game_title: str):
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM games WHERE title LIKE %s", (f"%{game_title}%",))
        games = cursor.fetchall()
        cursor.close()
        conn.close()
        if not games:
            raise HTTPException(status_code=404, detail="Aucun jeu trouvé avec ce titre.")
        return {"games": games}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur interne : {str(e)}")

@app.get("/games/price/{min_price}/{max_price}", dependencies=[Depends(verify_token)])
def get_games_by_price(min_price: float, max_price: float):
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        query = """
            SELECT g.title, bp.best_price_PC, bp.best_shop_PC, bp.site_url_PC, 
                   GROUP_CONCAT(p.platform_name SEPARATOR ', ') AS platforms
            FROM best_price_pc bp
            JOIN games g ON g.title = bp.title
            LEFT JOIN game_platforms gp ON g.game_id_rawg = gp.game_id_rawg
            LEFT JOIN platforms p ON gp.platform_id = p.platform_id
            WHERE bp.best_price_PC BETWEEN %s AND %s
            GROUP BY g.title, bp.best_price_PC, bp.best_shop_PC, bp.site_url_PC;
        """
        cursor.execute(query, (min_price, max_price))
        games = cursor.fetchall()
        cursor.close()
        conn.close()
        if not games:
            raise HTTPException(status_code=404, detail="Aucun jeu trouvé dans cette fourchette de prix.")
        return {"games": games}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur interne : {str(e)}")

@app.get("/platforms", dependencies=[Depends(verify_token)])
def get_all_platforms():
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM platforms")
        platforms = cursor.fetchall()
        cursor.close()
        conn.close()
        if not platforms:
            raise HTTPException(status_code=404, detail="Aucune plateforme trouvée.")
        return {"platforms": platforms}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur interne : {str(e)}")

@app.get("/games/platform_name/{platform_name}", dependencies=[Depends(verify_token)])
def get_games_by_platform_name(platform_name: str):
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        query = """
            SELECT g.title, g.release_date
            FROM games g
            JOIN game_platforms gp ON g.game_id_rawg = gp.game_id_rawg
            JOIN platforms p ON gp.platform_id = p.platform_id
            WHERE p.platform_name LIKE %s
        """
        cursor.execute(query, (f"%{platform_name}%",))
        games = cursor.fetchall()
        cursor.close()
        conn.close()
        if not games:
            raise HTTPException(status_code=404, detail="Aucun jeu trouvé pour cette plateforme.")
        return {"games": games}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur interne : {str(e)}")

@app.get("/games/genre/{genre_name}", dependencies=[Depends(verify_token)])
def get_games_by_genre(genre_name: str):
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM games WHERE genres LIKE %s", (f"%{genre_name}%",))
        games = cursor.fetchall()
        cursor.close()
        conn.close()
        if not games:
            raise HTTPException(status_code=404, detail=f"Aucun jeu trouvé pour le genre '{genre_name}'.")
        filtered_games = []
        for game in games:
            if game['genres']:
                genre_list = [g.strip().lower() for g in game['genres'].split(',')]
                if genre_name.lower() in genre_list:
                    filtered_games.append(game)
        if not filtered_games:
            raise HTTPException(status_code=404, detail=f"Aucun jeu trouvé pour le genre exact '{genre_name}'.")
        return {"games": filtered_games}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur interne : {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)