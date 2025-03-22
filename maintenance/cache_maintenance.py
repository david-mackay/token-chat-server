import threading
import time

def setup_cache_maintenance(chat_manager, interval_minutes=30):
    """
    Set up a background thread to periodically clean up the profile cache
    
    Args:
        chat_manager: The ChatManager instance
        interval_minutes: How often to run the cleanup (in minutes)
    """
    def cleanup_task():
        while True:
            # Sleep first to allow the application to initialize
            time.sleep(interval_minutes * 60)
            
            try:
                before_count = chat_manager.get_cached_profile_count()
                chat_manager.clear_expired_cache()
                after_count = chat_manager.get_cached_profile_count()
                
                print(f"Cache cleanup: removed {before_count - after_count} expired profiles. "
                      f"Current cache size: {after_count}")
            except Exception as e:
                print(f"Error during cache cleanup: {e}")
    
    # Create and start the thread
    cleanup_thread = threading.Thread(target=cleanup_task, daemon=True)
    cleanup_thread.start()
    
    print(f"Started profile cache maintenance task (cleanup every {interval_minutes} minutes)")
    
    return cleanup_thread