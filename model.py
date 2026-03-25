import os
import gc
import torch
import logging
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig

logger = logging.getLogger(__name__)


def initialize_qwen_model():
    """
    Инициализация Qwen-модели с 4-bit квантованием (если доступна GPU).
    По умолчанию используется компактная версия, которая обычно скачивается одним файлом.
    Автоматически определяет GPU/CPU.
    Возвращает (tokenizer, model) или (None, None) при ошибке.
    """
    # Очистка перед работой
    gc.collect()
    torch.cuda.empty_cache()
    
    try:
        model_name = os.getenv("QWEN_MODEL_NAME", "Qwen/Qwen2.5-Coder-7B")
        MODEL_CACHE_DIR = os.path.join(os.path.dirname(__file__), "model")
        os.makedirs(MODEL_CACHE_DIR, exist_ok=True)

        logger.info(f"Загрузка модели: {model_name}")
        logger.info(f"Кэш: {MODEL_CACHE_DIR}")

        # Определяем устройство
        has_cuda = torch.cuda.is_available()
        device = "cuda" if has_cuda else "cpu"
        logger.info(f"Доступное устройство: {device}")

        tokenizer = AutoTokenizer.from_pretrained(
            model_name,
            cache_dir=MODEL_CACHE_DIR,
            trust_remote_code=True
        )

        # Конфиг квантования (только если есть GPU)
        quantization_config = None
        if has_cuda:
            quantization_config = BitsAndBytesConfig(
                load_in_4bit=True,
                bnb_4bit_compute_dtype=torch.float16,
                bnb_4bit_quant_type="nf4",
                bnb_4bit_use_double_quant=True
            )

        model = AutoModelForCausalLM.from_pretrained(
            model_name,
            cache_dir=MODEL_CACHE_DIR,
            quantization_config=quantization_config,
            device_map="auto" if has_cuda else None,
            torch_dtype=torch.float16 if has_cuda else torch.float32,
            trust_remote_code=True,
            low_cpu_mem_usage=True,
            use_cache=False
        )

        if not has_cuda:
            model.to("cpu")

        model.eval()
        if hasattr(model, "generation_config"):
            # Для deterministic/greedy генерации без предупреждений о sampling флагах.
            model.generation_config.temperature = None
            model.generation_config.top_p = None
            model.generation_config.top_k = None

        logger.info("✅ Модель успешно загружена")
        return tokenizer, model

    except Exception as e:
        logger.exception(f"❌ Ошибка загрузки модели: {e}")
        return None, None


# Глобальная инициализация
qwen_tokenizer, qwen_model = initialize_qwen_model()
